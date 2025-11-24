package com.twilight.backend.aspect;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;

/**
 * Controller层统一日志切面
 * 记录请求参数和返回结果
 */
@Slf4j
@Aspect
@Component
public class ControllerLoggingAspect {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Pointcut("execution(* com.twilight.backend.controller..*(..))")
    public void controllerMethods() {
    }

    @Around("controllerMethods()")
    public Object logAround(ProceedingJoinPoint joinPoint) throws Throwable {
        long startTime = System.currentTimeMillis();
        
        // 获取请求信息
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        if (attributes == null) {
            return joinPoint.proceed();
        }
        
        HttpServletRequest request = attributes.getRequest();
        String method = request.getMethod();
        String uri = request.getRequestURI();
        String queryString = request.getQueryString();
        
        // 获取方法参数
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        String[] parameterNames = signature.getParameterNames();
        Object[] args = joinPoint.getArgs();
        
        Map<String, Object> params = new HashMap<>();
        if (parameterNames != null && args != null) {
            for (int i = 0; i < parameterNames.length; i++) {
                // 过滤掉HttpServletRequest等非业务参数
                if (args[i] != null && 
                    !args[i].getClass().getName().startsWith("javax.servlet") &&
                    !args[i].getClass().getName().startsWith("org.springframework")) {
                    params.put(parameterNames[i], args[i]);
                }
            }
        }
        
        // 记录请求日志
        log.info("==> {} {} {} | Params: {}", 
                method, 
                uri, 
                queryString != null ? "?" + queryString : "",
                params.isEmpty() ? "{}" : formatParams(params));
        
        Object result = null;
        try {
            // 执行方法
            result = joinPoint.proceed();
            
            // 记录响应日志
            long duration = System.currentTimeMillis() - startTime;
            log.info("<== {} {} | Duration: {}ms | Result: {}", 
                    method, 
                    uri, 
                    duration,
                    formatResult(result));
            
            return result;
        } catch (Exception ex) {
            long duration = System.currentTimeMillis() - startTime;
            log.error("<== {} {} | Duration: {}ms | Error: {}", 
                    method, 
                    uri, 
                    duration,
                    ex.getMessage());
            throw ex;
        }
    }
    
    private String formatParams(Map<String, Object> params) {
        try {
            return objectMapper.writeValueAsString(params);
        } catch (Exception e) {
            return params.toString();
        }
    }
    
    private String formatResult(Object result) {
        if (result == null) {
            return "null";
        }
        
        try {
            String json = objectMapper.writeValueAsString(result);
            // 如果返回结果太长，只显示前200个字符
            if (json.length() > 200) {
                return json.substring(0, 200) + "... (truncated)";
            }
            return json;
        } catch (Exception e) {
            return result.getClass().getSimpleName();
        }
    }
}



