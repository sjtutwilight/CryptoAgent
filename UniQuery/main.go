package main

import (
	"log"
	"net/http"
	"os"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/joho/godotenv"
	"github.com/rs/cors"
	"github.com/yangguang/Twilight/UniQuery/db"
	"github.com/yangguang/Twilight/UniQuery/graph/generated"
	"github.com/yangguang/Twilight/UniQuery/resolvers"
)

func main() {
	// 加载环境变量
	err := godotenv.Load()
	if err != nil {
		log.Println("警告: 未找到.env文件，使用默认环境变量")
	}

	// 初始化数据库连接
	db.InitDB()
	defer db.CloseDB()

	// 检查token_holder表中是否有数据
	err = db.CheckTokenHolderData()
	if err != nil {
		log.Printf("检查token_holder表数据失败: %v", err)
	}

	// 创建根解析器
	resolver := resolvers.NewRootResolver()
	log.Printf("根解析器已创建: %v", resolver != nil)

	// 创建GraphQL处理器
	srv := handler.NewDefaultServer(generated.NewExecutableSchema(generated.Config{
		Resolvers: resolver,
	}))

	// 添加传输和扩展
	srv.AddTransport(transport.POST{})
	srv.AddTransport(transport.GET{})
	srv.Use(extension.Introspection{})

	// 创建CORS处理器
	corsHandler := cors.New(cors.Options{
		AllowedOrigins:   []string{"http://localhost:3000", "http://localhost:8085"},
		AllowCredentials: true,
		AllowedMethods:   []string{"GET", "POST", "OPTIONS"},
		AllowedHeaders:   []string{"Authorization", "Content-Type", "Apollo-Require-Preflight"},
		Debug:            true,
	})

	// 创建路由器
	router := http.NewServeMux()
	router.Handle("/", playground.Handler("GraphQL playground", "/query"))
	router.Handle("/query", srv)

	// 应用CORS中间件
	handler := corsHandler.Handler(router)

	// 获取端口
	port := os.Getenv("PORT")
	if port == "" {
		port = "8085"
	}

	// 启动服务器
	log.Printf("连接到 http://localhost:%s/ 查看 GraphQL playground", port)
	log.Fatal(http.ListenAndServe(":"+port, handler))
}
