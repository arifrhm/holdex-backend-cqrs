package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DBDSN                 string
	DBDSNRead             string
	RedisAddr             string
	Port                  string
	GRPCPort              string
	CoinGeckoPollInterval time.Duration
	LogLevel              string
	SkipMigrations        bool
	DBMaxConns            int
	DBMinConns            int
	DBMaxConnIdleTime     time.Duration
	RateLimitRPS          float64
	RateLimitBurst        int
	TrustProxy            bool
	APIKey                string
	RedisPassword         string
	CoinGeckoAPIKey       string
	CoinGeckoCoins        []string
}

func Load() *Config {
	dbDSN := getEnv("DB_DSN", "postgres://holdex_user:holdex_password@localhost:5433/holdex_db?sslmode=disable")
	dbDSNRead := getEnv("DB_DSN_READ", dbDSN) // Default to DB_DSN if not set
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	port := getEnv("PORT", "8080")
	grpcPort := getEnv("GRPC_PORT", "9090")
	logLevel := getEnv("LOG_LEVEL", "info")
	skipMigrate := getEnv("SKIP_MIGRATIONS", "false") == "true"

	dbMaxConns, err := strconv.Atoi(getEnv("DB_MAX_CONNS", "10"))
	if err != nil {
		dbMaxConns = 10
	}
	dbMinConns, err := strconv.Atoi(getEnv("DB_MIN_CONNS", "2"))
	if err != nil {
		dbMinConns = 2
	}
	idleTimeStr := getEnv("DB_MAX_CONN_IDLE_TIME", "5m")
	idleTime, err := time.ParseDuration(idleTimeStr)
	if err != nil {
		idleTime = 5 * time.Minute
	}

	pollStr := getEnv("COINGECKO_POLL_INTERVAL", "60s")
	pollInterval, err := time.ParseDuration(pollStr)
	if err != nil {
		pollInterval = 60 * time.Second
	}

	rateRPS, err := strconv.ParseFloat(getEnv("RATE_LIMIT_RPS", "50.0"), 64)
	if err != nil {
		rateRPS = 50.0
	}
	rateBurst, err := strconv.Atoi(getEnv("RATE_LIMIT_BURST", "10"))
	if err != nil {
		rateBurst = 10
	}

	trustProxy := getEnv("TRUST_PROXY", "false") == "true"
	apiKey := getEnv("API_KEY", "")
	redisPassword := getEnv("REDIS_PASSWORD", "")

	cgAPIKey := getEnv("COINGECKO_API_KEY", "")
	coinsStr := getEnv("COINGECKO_COINS", "bitcoin,ethereum,cardano,solana,ripple")
	coins := strings.Split(coinsStr, ",")
	for i, c := range coins {
		coins[i] = strings.TrimSpace(c)
	}

	return &Config{
		DBDSN:                 dbDSN,
		DBDSNRead:             dbDSNRead,
		RedisAddr:             redisAddr,
		Port:                  port,
		GRPCPort:              grpcPort,
		CoinGeckoPollInterval: pollInterval,
		LogLevel:              logLevel,
		SkipMigrations:        skipMigrate,
		DBMaxConns:            dbMaxConns,
		DBMinConns:            dbMinConns,
		DBMaxConnIdleTime:     idleTime,
		RateLimitRPS:          rateRPS,
		RateLimitBurst:        rateBurst,
		TrustProxy:            trustProxy,
		APIKey:                apiKey,
		RedisPassword:         redisPassword,
		CoinGeckoAPIKey:       cgAPIKey,
		CoinGeckoCoins:        coins,
	}
}

// String implements the fmt.Stringer interface to print Config attributes while masking sensitive passwords in database DSNs
func (c *Config) String() string {
	return fmt.Sprintf(
		"Config{Port: %s, GRPCPort: %s, LogLevel: %s, DBDSN: %s, DBDSNRead: %s, SkipMigrations: %t, DBMaxConns: %d, DBMinConns: %d, DBMaxConnIdleTime: %s, CoinGeckoPollInterval: %s, CoinGeckoCoins: %v, HasCoinGeckoAPIKey: %t, RateLimitRPS: %.2f, RateLimitBurst: %d}",
		c.Port,
		c.GRPCPort,
		c.LogLevel,
		maskDSN(c.DBDSN),
		maskDSN(c.DBDSNRead),
		c.SkipMigrations,
		c.DBMaxConns,
		c.DBMinConns,
		c.DBMaxConnIdleTime,
		c.CoinGeckoPollInterval,
		c.CoinGeckoCoins,
		c.CoinGeckoAPIKey != "",
		c.RateLimitRPS,
		c.RateLimitBurst,
	)
}

func maskDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return "postgres://[parse-failed]"
	}
	if u.User != nil {
		if _, hasPass := u.User.Password(); hasPass {
			u.User = url.UserPassword(u.User.Username(), "xxxx")
		}
	}
	return u.String()
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}
