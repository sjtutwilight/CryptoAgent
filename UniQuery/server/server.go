package server

import (
	"log"
	"net/http"
	"os"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/joho/godotenv"
	"github.com/yangguang/Twilight/UniQuery/db"
	"github.com/yangguang/Twilight/UniQuery/resolvers"
)

// StartServer 启动GraphQL服务器
func StartServer() {
	// 加载环境变量
	err := godotenv.Load()
	if err != nil {
		log.Println("警告: 未找到.env文件，使用默认环境变量")
	}

	// 初始化数据库连接
	db.InitDB()
	defer db.CloseDB()

	// 创建查询解析器
	resolver := resolvers.NewQueryResolver()

	// 创建GraphQL处理器
	srv := handler.NewDefaultServer(NewExecutableSchema(Config{
		Resolvers: resolver,
	}))

	// 创建GraphQL playground
	http.Handle("/", playground.Handler("GraphQL playground", "/query"))
	http.Handle("/query", srv)

	// 获取端口
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// 启动服务器
	log.Printf("连接到 http://localhost:%s/ 查看 GraphQL playground", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
