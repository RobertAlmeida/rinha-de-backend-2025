package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

// Estruturas de dados
type PaymentRequest struct {
	CorrelationID string  `json:"correlationId" binding:"required"`
	Amount        float64 `json:"amount" binding:"required"`
}

type PaymentResponse struct {
	Message string `json:"message"`
}

type PaymentSummaryResponse struct {
	Default  ProcessorSummary `json:"default"`
	Fallback ProcessorSummary `json:"fallback"`
}

type ProcessorSummary struct {
	TotalRequests int     `json:"totalRequests"`
	TotalAmount   float64 `json:"totalAmount"`
}

type HealthCheckResponse struct {
	Failing         bool `json:"failing"`
	MinResponseTime int  `json:"minResponseTime"`
}

type HealthCheckCache struct {
	Failing         bool      `json:"failing"`
	MinResponseTime int       `json:"minResponseTime"`
	LastCheckedAt   time.Time `json:"lastCheckedAt"`
}

// Variáveis globais
var (
	redisClient     *redis.Client
	healthCache     = make(map[string]*HealthCheckCache)
	healthCacheMux  sync.RWMutex
	httpClient      = &http.Client{Timeout: 10 * time.Second}
	defaultPPURL    = "http://payment-processor-default:8080"
	fallbackPPURL   = "http://payment-processor-fallback:8080"
)

func init() {
	// Configurar URLs dos Payment Processors a partir de variáveis de ambiente
	if url := os.Getenv("PAYMENT_PROCESSOR_URL_DEFAULT"); url != "" {
		defaultPPURL = url
	}
	if url := os.Getenv("PAYMENT_PROCESSOR_URL_FALLBACK"); url != "" {
		fallbackPPURL = url
	}
}

func main() {
	// Inicializar Redis
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	redisClient = redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	// Testar conexão com Redis
	ctx := context.Background()
	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		log.Printf("Aviso: Não foi possível conectar ao Redis: %v. Usando cache em memória.", err)
		redisClient = nil
	}

	// Configurar Gin
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()

	// Configurar CORS
	config := cors.DefaultConfig()
	config.AllowAllOrigins = true
	config.AllowMethods = []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"}
	config.AllowHeaders = []string{"*"}
	r.Use(cors.New(config))

	// Rotas
	r.POST("/payments", handlePayments)
	r.GET("/payments-summary", handlePaymentsSummary)

	// Inicializar cache de health-check
	initHealthCache()

	// Iniciar servidor
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Servidor iniciando na porta %s", port)
	log.Fatal(r.Run("0.0.0.0:" + port))
}

func initHealthCache() {
	healthCacheMux.Lock()
	defer healthCacheMux.Unlock()

	healthCache["default"] = &HealthCheckCache{
		Failing:         false,
		MinResponseTime: 100,
		LastCheckedAt:   time.Time{},
	}

	healthCache["fallback"] = &HealthCheckCache{
		Failing:         false,
		MinResponseTime: 200,
		LastCheckedAt:   time.Time{},
	}
}

func handlePayments(c *gin.Context) {
	var req PaymentRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Validar UUID
	if _, err := uuid.Parse(req.CorrelationID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "correlationId deve ser um UUID válido"})
		return
	}

	// Responder imediatamente ao cliente
	c.JSON(http.StatusOK, PaymentResponse{Message: "payment received"})

	// Processar pagamento de forma assíncrona
	go processPayment(req)
}

func processPayment(req PaymentRequest) {
	// Selecionar o melhor Payment Processor
	processor := selectBestProcessor()

	// Preparar requisição para o PP
	ppReq := map[string]interface{}{
		"correlationId": req.CorrelationID,
		"amount":        req.Amount,
		"requestedAt":   time.Now().UTC().Format(time.RFC3339),
	}

	// Tentar processar com o PP selecionado
	success := sendToProcessor(processor, ppReq)

	// Se falhou com o default, tentar com o fallback
	if !success && processor == "default" {
		log.Printf("Falha no processor default, tentando fallback para %s", req.CorrelationID)
		success = sendToProcessor("fallback", ppReq)
		if success {
			processor = "fallback"
		}
	}

	// Atualizar contadores se o pagamento foi processado com sucesso
	if success {
		updateSummaryCounters(processor, req.Amount)
		log.Printf("Pagamento %s processado com sucesso pelo %s", req.CorrelationID, processor)
	} else {
		log.Printf("Falha ao processar pagamento %s", req.CorrelationID)
	}
}

func selectBestProcessor() string {
	defaultHealth := getHealthCheck("default")

	// Se o default não está falhando, usar ele (menor taxa)
	if !defaultHealth.Failing {
		return "default"
	}

	// Se o default está falhando, usar o fallback
	return "fallback"
}

func getHealthCheck(processor string) *HealthCheckCache {
	healthCacheMux.RLock()
	cached := healthCache[processor]
	healthCacheMux.RUnlock()

	// Se não há cache ou está expirado (mais de 5 segundos), atualizar
	if cached == nil || time.Since(cached.LastCheckedAt) > 5*time.Second {
		updateHealthCheck(processor)
		healthCacheMux.RLock()
		cached = healthCache[processor]
		healthCacheMux.RUnlock()
	}

	return cached
}

func updateHealthCheck(processor string) {
	var url string
	if processor == "default" {
		url = defaultPPURL + "/payments/service-health"
	} else {
		url = fallbackPPURL + "/payments/service-health"
	}

	resp, err := httpClient.Get(url)
	if err != nil {
		log.Printf("Erro ao verificar health do %s: %v", processor, err)
		// Marcar como falhando se não conseguir conectar
		healthCacheMux.Lock()
		healthCache[processor] = &HealthCheckCache{
			Failing:         true,
			MinResponseTime: 1000,
			LastCheckedAt:   time.Now(),
		}
		healthCacheMux.Unlock()
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		// Limite de rate excedido, não atualizar o cache
		log.Printf("Rate limit excedido para health check do %s", processor)
		return
	}

	var healthResp HealthCheckResponse
	if err := json.NewDecoder(resp.Body).Decode(&healthResp); err != nil {
		log.Printf("Erro ao decodificar health response do %s: %v", processor, err)
		return
	}

	healthCacheMux.Lock()
	healthCache[processor] = &HealthCheckCache{
		Failing:         healthResp.Failing,
		MinResponseTime: healthResp.MinResponseTime,
		LastCheckedAt:   time.Now(),
	}
	healthCacheMux.Unlock()

	log.Printf("Health check atualizado para %s: failing=%v, minResponseTime=%d", 
		processor, healthResp.Failing, healthResp.MinResponseTime)
}

func sendToProcessor(processor string, ppReq map[string]interface{}) bool {
	var url string
	if processor == "default" {
		url = defaultPPURL + "/payments"
	} else {
		url = fallbackPPURL + "/payments"
	}

	jsonData, err := json.Marshal(ppReq)
	if err != nil {
		log.Printf("Erro ao serializar requisição: %v", err)
		return false
	}

	// Retry com backoff exponencial
	maxRetries := 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		resp, err := httpClient.Post(url, "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			log.Printf("Erro na tentativa %d para %s: %v", attempt+1, processor, err)
			if attempt < maxRetries-1 {
				time.Sleep(time.Duration(1<<attempt) * time.Second) // Backoff exponencial
				continue
			}
			return false
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return true
		}

		log.Printf("Status code %d na tentativa %d para %s", resp.StatusCode, attempt+1, processor)
		if attempt < maxRetries-1 {
			time.Sleep(time.Duration(1<<attempt) * time.Second) // Backoff exponencial
		}
	}

	return false
}

func updateSummaryCounters(processor string, amount float64) {
	ctx := context.Background()

	if redisClient != nil {
		// Usar Redis para persistência
		key := fmt.Sprintf("summary:%s", processor)
		pipe := redisClient.Pipeline()
		pipe.HIncrBy(ctx, key, "totalRequests", 1)
		pipe.HIncrByFloat(ctx, key, "totalAmount", amount)
		_, err := pipe.Exec(ctx)
		if err != nil {
			log.Printf("Erro ao atualizar contadores no Redis: %v", err)
		}
	} else {
		// Fallback para contadores em memória (não persistente)
		log.Printf("Atualizando contadores em memória para %s: amount=%.2f", processor, amount)
	}
}

func handlePaymentsSummary(c *gin.Context) {
	// Parâmetros opcionais de filtro por data (não implementados nesta versão inicial)
	// from := c.Query("from")
	// to := c.Query("to")

	summary := PaymentSummaryResponse{
		Default:  getProcessorSummary("default"),
		Fallback: getProcessorSummary("fallback"),
	}

	c.JSON(http.StatusOK, summary)
}

func getProcessorSummary(processor string) ProcessorSummary {
	ctx := context.Background()

	if redisClient != nil {
		key := fmt.Sprintf("summary:%s", processor)
		
		totalRequestsStr, err := redisClient.HGet(ctx, key, "totalRequests").Result()
		if err != nil && err != redis.Nil {
			log.Printf("Erro ao obter totalRequests do Redis: %v", err)
		}

		totalAmountStr, err := redisClient.HGet(ctx, key, "totalAmount").Result()
		if err != nil && err != redis.Nil {
			log.Printf("Erro ao obter totalAmount do Redis: %v", err)
		}

		totalRequests := 0
		totalAmount := 0.0

		if totalRequestsStr != "" {
			if val, err := strconv.Atoi(totalRequestsStr); err == nil {
				totalRequests = val
			}
		}

		if totalAmountStr != "" {
			if val, err := strconv.ParseFloat(totalAmountStr, 64); err == nil {
				totalAmount = val
			}
		}

		return ProcessorSummary{
			TotalRequests: totalRequests,
			TotalAmount:   totalAmount,
		}
	}

	// Fallback para valores zerados se não há Redis
	return ProcessorSummary{
		TotalRequests: 0,
		TotalAmount:   0.0,
	}
}

