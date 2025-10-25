# CRM Batch Service Port Fallback Mechanism

## Overview

This document describes the port fallback mechanism implemented for the CRM Batch Service to handle dynamic port changes in cluster environments.

## Problem Statement

In cluster environments (Kubernetes, Docker Swarm, etc.), services may run on different ports across different nodes or deployments. The CRM Batch Service (`CRM_BATCH_URL`) is configured with a single port (e.g., 28080), but when that port becomes unavailable, the scheduled task fails without retry.

**Before**: Single port → Connection failure → Task fails

**After**: Primary port → Fallback ports → Success or graceful error

## Architecture

### Components Modified

1. **`src/util/http_utils.py`** - Core HTTP utilities with port fallback logic
2. **`src/settings/app.py`** - Configuration management for fallback ports
3. **`src/crm/flow.py`** - CRM data processing pipeline using the new mechanism
4. **`.env.sample`** - Environment configuration documentation

### Port Fallback Flow

```
Request to CRM_BATCH_URL (primary port)
         ↓
    [Success] → Return response
         ↓ [Connection Error / Timeout]
    Try Fallback Port 1 (8080)
         ↓ [Success] → Return response
         ↓ [Connection Error / Timeout]
    Try Fallback Port 2 (28080)
         ↓ [Success] → Return response
         ↓ [Connection Error / Timeout]
    Try Fallback Port 3 (18080)
         ↓ [Success] → Return response
         ↓ [Connection Error / Timeout]
    All ports exhausted → Error response with details
```

## API Reference

### Synchronous Function (for CRM batch operations)

```python
def post_json_data_sync_with_port_fallback(
    endpoint: str,
    data: Union[Dict[str, Any], List[Dict[str, Any]]],
    token: Optional[str] = None,
    timeout: float = 30.0,
    fallback_ports: Optional[List[int]] = None,
) -> Dict[str, Any]:
```

**Parameters:**
- `endpoint`: Full URL or path to the API endpoint
- `data`: JSON data to send (dict or list of dicts)
- `token`: Authentication token (optional, defaults from settings)
- `timeout`: Request timeout in seconds (default: 30)
- `fallback_ports`: List of ports to try if primary fails (optional, defaults from `CRM_BATCH_FALLBACK_PORTS`)

**Returns:**
```python
# Success response
{
    "status": "success",
    "...": "response data from server"
}

# Error response
{
    "status": "error",
    "message": "Cannot connect to any port: <error details>",
    "ports_tried": [28080, 8080, 28080, 18080]
}
```

**Example Usage:**
```python
from src.util.http_utils import post_json_data_sync_with_port_fallback

response = post_json_data_sync_with_port_fallback(
    endpoint="http://127.0.0.1:28080/BatchService/submit_batch",
    data=batch_data,
    timeout=60
)

if response.get("status") == "error":
    print(f"Failed: {response.get('message')}")
else:
    print("Success")
```

### Async Function (for future use)

```python
async def post_json_data_with_port_fallback(
    endpoint: str,
    data: Union[Dict[str, Any], List[Dict[str, Any]]],
    token: Optional[str] = None,
    timeout: float = 30.0,
    fallback_ports: Optional[List[int]] = None,
) -> Dict[str, Any]:
```

Same signature and behavior as synchronous version, but async/await compatible.

## Configuration

### Environment Variables

```bash
# Primary CRM Batch Service URL
CRM_BATCH_URL=http://127.0.0.1:28080/BatchService/submit_batch

# Fallback ports to try (comma-separated)
# Default: 8080,28080,18080
CRM_BATCH_FALLBACK_PORTS=8080,28080,18080

# Request timeout in seconds (default: 60)
CRM_BATCH_TIMEOUT=60
```

### Example .env Configuration

```bash
# Standard cluster setup
CRM_BATCH_URL=http://batch-service.default.svc.cluster.local:28080/BatchService/submit_batch
CRM_BATCH_FALLBACK_PORTS=8080,28080,18080
CRM_BATCH_TIMEOUT=60

# Custom configuration
CRM_BATCH_FALLBACK_PORTS=9000,9001,9002
```

### Accessing from Python

```python
from src.settings import app_settings

# Get fallback ports
fallback_ports = app_settings.get_crm_batch_fallback_ports()
# Returns: [8080, 28080, 18080]

# Or pass custom ports when calling
response = post_json_data_sync_with_port_fallback(
    endpoint=url,
    data=data,
    fallback_ports=[9000, 9001, 9002]
)
```

## Implementation Details

### Port Replacement Logic

The `_replace_port()` helper function safely replaces the port in a URL:

```python
original: "http://127.0.0.1:28080/BatchService/submit_batch"
port 8080 → "http://127.0.0.1:8080/BatchService/submit_batch"
port 18080 → "http://127.0.0.1:18080/BatchService/submit_batch"
```

### Error Handling

The mechanism catches:
- `httpx.ConnectError` - Connection refused
- `httpx.TimeoutException` - Request timeout
- `httpx.HTTPError` - General HTTP errors (4xx, 5xx)
- Network errors
- DNS resolution failures

For each failed port, a warning is logged:
```
Request failed to port 28080: ConnectError - Connection refused. Trying next port...
Request failed to port 8080: TimeoutException - Request timeout. Trying next port...
Request failed to port 18080: HTTPError - 503 Service Unavailable. No more ports to try.
```

### Logging

All attempts are logged at different levels:

```
INFO: Attempting POST to http://127.0.0.1:28080/BatchService/submit_batch (attempt 1/4)
WARNING: Request failed to port 28080: ConnectError. Trying next port...
INFO: Attempting POST to http://127.0.0.1:8080/BatchService/submit_batch (attempt 2/4)
INFO: POST thành công đến http://127.0.0.1:8080/BatchService/submit_batch
```

## Integration with CRM Batch Processing

The `submit_batch()` function in `src/crm/flow.py` has been updated to use the port fallback mechanism:

```python
def submit_batch(batch_data: List[Dict]) -> Dict:
    """Submit the transformed data to the batch service in smaller chunks with port fallback"""

    for chunk in chunks:
        response = post_json_data_sync_with_port_fallback(
            endpoint=BATCH_URL,
            data=chunk,
            timeout=BATCH_TIMEOUT
        )

        if response.get("status") == "error":
            logger.error(f"Failed to submit batch: {response.get('message')}")
            raise
```

## Celery Task Integration

The scheduled CRM sync task (via Celery) will now:

1. Process data normally
2. Call `submit_batch()` which uses port fallback
3. Automatically retry different ports on connection failures
4. Log all attempts for debugging
5. Return detailed error info on complete failure

**Scheduled Task**: `sync_crm_data` (11:30 AM daily)

Configuration in `src/tasks/worker.py`:
```python
@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def sync_crm_data(self):
    result = process_data()  # Uses port fallback internally
```

## Testing

### Manual Testing

```python
# Test with explicit ports
from src.util.http_utils import post_json_data_sync_with_port_fallback

response = post_json_data_sync_with_port_fallback(
    endpoint="http://127.0.0.1:28080/test",
    data={"test": "data"},
    fallback_ports=[8080, 28080, 18080]
)

print(response["status"])  # "success" or "error"
print(response.get("ports_tried"))  # List of attempted ports
```

### Debugging

Enable detailed logging:
```python
from src.util.logging import get_logger
import logging

# Set to DEBUG level
logging.getLogger("src.util.http_utils").setLevel(logging.DEBUG)
```

### Unit Test Example

```python
import pytest
from src.util.http_utils import post_json_data_sync_with_port_fallback

def test_port_fallback_success_on_second_port(mocker):
    """Test successful connection on fallback port"""
    mock_client = mocker.patch("httpx.Client")

    # First call fails, second succeeds
    mock_client.return_value.post.side_effect = [
        Exception("Connection refused"),
        mocker.Mock(status_code=200, json=lambda: {"success": True})
    ]

    response = post_json_data_sync_with_port_fallback(
        endpoint="http://localhost:28080/test",
        data={"test": "data"},
        fallback_ports=[8080, 18080]
    )

    assert response["status"] == "success"
```

## Performance Considerations

1. **Timeout Behavior**: Each port attempt uses the same timeout (default 30s)
   - 3 fallback ports = up to 90 seconds total
   - Configure `CRM_BATCH_TIMEOUT` appropriately

2. **Retry Strategy**:
   - No built-in delay between port attempts (instant)
   - Celery handles task retries at a higher level (60s delay, 3 max retries)

3. **Connection Pooling**: Each attempt uses a fresh HTTP client
   - No connection reuse (safe for changing ports)
   - May be optimized in future if needed

## Troubleshooting

### Scenario 1: All ports fail

**Log Output:**
```
ERROR: Tất cả cổng fallback đều thất bại. Last error: Connection refused
```

**Solution:**
1. Verify CRM Batch Service is running: `docker ps | grep batch`
2. Check service is listening on one of the configured ports
3. Update `CRM_BATCH_FALLBACK_PORTS` to include the correct port
4. Check firewall rules for port access

### Scenario 2: Wrong endpoint path

**Log Output:**
```
WARNING: Request failed to port 8080: HTTPError - 404 Not Found
```

**Solution:**
1. Verify `CRM_BATCH_URL` path is correct
2. Check CRM Batch Service routing configuration
3. Confirm endpoint exists: `curl http://service:8080/BatchService/submit_batch`

### Scenario 3: Timeout on all ports

**Log Output:**
```
WARNING: Request failed to port 28080: TimeoutException - Request timeout
```

**Solution:**
1. Increase `CRM_BATCH_TIMEOUT` value
2. Check network connectivity to service
3. Verify service is not overloaded: `docker stats batch-service`
4. Check service logs: `docker logs batch-service`

## Migration Notes

### For Existing Deployments

1. **No Breaking Changes**: The new mechanism is backward compatible
   - Existing code continues to work
   - Default fallback ports are [8080, 28080, 18080]

2. **Update Steps**:
   ```bash
   # 1. Update environment variables in .env
   CRM_BATCH_FALLBACK_PORTS=8080,28080,18080

   # 2. No code changes needed
   # 3. Restart services
   docker-compose restart
   ```

3. **Verification**:
   ```bash
   # Check logs for port fallback messages
   docker logs lug-back | grep "Attempting POST"
   docker logs lug-back | grep "Fallback"
   ```

## Security Considerations

- **No exposed credentials**: Ports are internal to cluster
- **HTTPS not supported**: Current implementation uses HTTP
  - For production, consider implementing HTTPS port fallback
- **DNS caching**: Service name resolution may be cached
  - Docker DNS caching may cause delays between port attempts

## Future Enhancements

1. **HTTPS Support**: Add port fallback for HTTPS endpoints
2. **Health Check**: Implement service health check before retry
3. **Exponential Backoff**: Add delays between retry attempts
4. **Circuit Breaker**: Skip failed ports for a period of time
5. **Metrics**: Track port success rates for monitoring

## Questions & Support

For implementation details or issues, refer to:
- **HTTP Utils**: `src/util/http_utils.py` (lines 16-117 async, 248-331 sync)
- **Settings**: `src/settings/app.py` (lines 109-135)
- **CRM Flow**: `src/crm/flow.py` (lines 524-585)
- **Configuration**: `.env.sample` (lines 55-61)
