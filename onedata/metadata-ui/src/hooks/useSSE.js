import { useEffect, useRef, useState, useCallback } from 'react';
import { subscribeUpdates } from '../services/api';

/**
 * SSE 实时更新订阅 Hook
 * 管理 EventSource 连接生命周期
 */
export const useSSE = (options = {}) => {
  const { 
    autoConnect = false,  // 是否自动连接
    onUpdate,             // 更新回调
    reconnectDelay = 5000 // 重连延迟
  } = options;

  const eventSourceRef = useRef(null);
  const reconnectTimeoutRef = useRef(null);
  
  const [connected, setConnected] = useState(false);
  const [lastUpdate, setLastUpdate] = useState(null);
  const [updateCount, setUpdateCount] = useState(0);

  // 处理消息
  const handleMessage = useCallback((data) => {
    setLastUpdate(data);
    setUpdateCount(prev => prev + 1);
    onUpdate?.(data);
  }, [onUpdate]);

  // 处理错误并重连
  const handleError = useCallback(() => {
    setConnected(false);
    // 清理当前连接
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
    // 延迟重连
    reconnectTimeoutRef.current = setTimeout(() => {
      connect();
    }, reconnectDelay);
  }, [reconnectDelay]);

  // 建立连接
  const connect = useCallback(() => {
    // 避免重复连接
    if (eventSourceRef.current) {
      return;
    }
    
    eventSourceRef.current = subscribeUpdates(handleMessage, handleError);
    
    eventSourceRef.current.onopen = () => {
      setConnected(true);
    };
  }, [handleMessage, handleError]);

  // 断开连接
  const disconnect = useCallback(() => {
    if (reconnectTimeoutRef.current) {
      clearTimeout(reconnectTimeoutRef.current);
      reconnectTimeoutRef.current = null;
    }
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
    setConnected(false);
  }, []);

  // 自动连接
  useEffect(() => {
    if (autoConnect) {
      connect();
    }
    return () => {
      disconnect();
    };
  }, [autoConnect, connect, disconnect]);

  // 重置更新计数
  const resetUpdateCount = useCallback(() => {
    setUpdateCount(0);
  }, []);

  return {
    connected,
    lastUpdate,
    updateCount,
    connect,
    disconnect,
    resetUpdateCount,
  };
};

export default useSSE;

