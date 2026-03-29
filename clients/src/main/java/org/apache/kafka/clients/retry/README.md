# Custom Retry Engine for Apache Kafka

This package provides a flexible retry engine for Apache Kafka clients, allowing you to experiment with different retry strategies beyond Kafka's built-in exponential backoff with jitter.

## Overview

The retry engine consists of:

- **RetryStrategy Interface**: Defines the contract for backoff calculations
- **Built-in Strategies**: Several implementations for common retry patterns
- **RetryEngine**: The core engine that executes operations with retries
- **RetryingKafkaProducer**: A producer wrapper that uses custom retry logic

## Components

### RetryStrategy

The `RetryStrategy` interface defines a single method:

```java
long backoff(int attempt);
```

Where `attempt` is the 0-based retry attempt number.

### Built-in Strategies

#### ExponentialBackoffStrategy
Implements exponential backoff with jitter, similar to Kafka's default.

**Formula**: `random(1 - jitter, 1 + jitter) * initialInterval * multiplier ^ attempt`

**Parameters**:
- `initialInterval`: Starting backoff time (ms)
- `multiplier`: Exponential growth factor
- `maxInterval`: Maximum backoff time (ms)
- `jitter`: Jitter factor (0.0 to 1.0)

#### FullJitterStrategy
Implements full jitter backoff.

**Formula**: `random(0, initialInterval * multiplier ^ attempt)`

**Parameters**:
- `initialInterval`: Starting backoff time (ms)
- `multiplier`: Exponential growth factor
- `maxInterval`: Maximum backoff time (ms)

#### FixedDelayStrategy
Simple fixed delay between retries.

**Parameters**:
- `delay`: Fixed delay in milliseconds

## Usage

### Basic Usage

```java
import org.apache.kafka.clients.retry.*;

// Create a retry strategy
RetryStrategy strategy = new FullJitterStrategy(100, 2, 1000);

// Create the retry engine
RetryEngine engine = new RetryEngine(strategy, 5);

// Execute an operation with retries
String result = engine.executeWithRetry(() -> {
    // Your operation that might throw RetriableException
    return someOperation();
});
```

### With Kafka Producer

```java
import org.apache.kafka.clients.retry.*;
import java.util.Properties;

// Configure producer properties (retries will be disabled internally)
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
// ... other props

// Choose strategy and max retries
RetryStrategy strategy = new ExponentialBackoffStrategy(100, 2, 1000, 0.2);
int maxRetries = 3;

// Create retrying producer
RetryingKafkaProducer<String, String> producer = new RetryingKafkaProducer<>(props, strategy, maxRetries);

// Send messages - retries happen automatically on network errors
producer.send(new ProducerRecord<>("my-topic", "key", "value"));
producer.close();
```

## How It Works

1. **Producer Configuration**: The `RetryingKafkaProducer` sets `retries=0` in the underlying Kafka producer to disable built-in retries.

2. **Retry Logic**: When `send()` is called, the `RetryEngine` wraps the operation and catches `RetriableException`s.

3. **Backoff Calculation**: On retryable failures, the engine calculates the backoff delay using the configured strategy and sleeps.

4. **Retry Loop**: The operation is retried up to `maxRetries` times, or until it succeeds or throws a non-retryable exception.

## Exception Handling

- **RetriableException**: Causes a retry with backoff
- **Other Exceptions**: Rethrown immediately without retry
- **ExecutionException**: Unwrapped to check the cause for retryability

## Comparison of Strategies

| Strategy | Backoff Pattern | Use Case |
|----------|----------------|----------|
| ExponentialBackoff | Exponential + jitter | Balanced load, default Kafka behavior |
| FullJitter | Random up to exponential | Avoid thundering herd, high concurrency |
| FixedDelay | Constant delay | Simple, predictable timing |

## Testing

Run the unit tests:

```bash
./gradlew :clients:test --tests RetryStrategyTest
```

## Extending

To create a custom retry strategy, implement the `RetryStrategy` interface:

```java
public class CustomStrategy implements RetryStrategy {
    @Override
    public long backoff(int attempt) {
        // Your backoff calculation
        return calculateDelay(attempt);
    }
}
```

## Notes

- The retry engine is synchronous - it blocks until the operation completes or all retries are exhausted.
- Only `RetriableException` and its subclasses trigger retries.
- The producer wrapper currently ignores callbacks in the `send(record, callback)` method for simplicity.</content>
<parameter name="filePath">c:\Users\kj2od\kafka\clients\src\main\java\org\apache\kafka\clients\retry\README.md