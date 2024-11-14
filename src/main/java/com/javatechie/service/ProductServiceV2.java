package com.javatechie.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.javatechie.entity.Product;
import com.javatechie.repository.ProductRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Service
public class ProductServiceV2 {

    private final ProductRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topicName;
    private final ExecutorService executorService = Executors.newFixedThreadPool(10); // 10 threads

    public ProductServiceV2(ProductRepository repository, KafkaTemplate<String, String> kafkaTemplate,
                            ObjectMapper objectMapper, @Value("${product.discount.update.topic}") String topicName) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topicName = topicName;
    }

    public void processProductIds(List<Long> productIds) {
        // Split product IDs into batches of 100
        List<List<Long>> batches = splitIntoBatches(productIds, 50);

        // Process each batch asynchronously using CompletableFuture
        List<CompletableFuture<Void>> futures = batches
                .stream()
                .map(batch ->
                        CompletableFuture.runAsync(() -> processBatch(batch), executorService))
                .toList();

        // Wait for all CompletableFutures to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    private void processBatch(List<Long> batch) {
        System.out.println("Processing batch "+batch+" on thread: " + Thread.currentThread().getName());
        batch.forEach(this::fetchUpdateAndPublish);
    }


    private void fetchUpdateAndPublish(Long productId) {
        Product product = repository.findById(productId)
                .orElseThrow(() -> new IllegalArgumentException("Product ID does not exist in the system"));

        //update discount properties
        product.setOfferApplied(true);
        updateDiscountedPrice(product);

        //save to DB
        repository.save(product);

        //kafka events
        publishProductEvent(product);
    }

    private void updateDiscountedPrice(Product product) {

        double price = product.getPrice();

        int discountPercentage = (price >= 1000) ? 10 : (price > 500 ? 5 : 0);

        double priceAfterDiscount = price - (price * discountPercentage / 100);

        product.setDiscountPercentage(discountPercentage);
        product.setPriceAfterDiscount(priceAfterDiscount);
    }

    private void publishProductEvent(Product product) {
        try {
            String productJson = objectMapper.writeValueAsString(product);
            kafkaTemplate.send(topicName, productJson);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to convert product to JSON", e);
        }
    }

    private List<List<Long>> splitIntoBatches(List<Long> productIds, int batchSize) {
        int totalSize = productIds.size();
        int numBatches = (totalSize + batchSize - 1) / batchSize; // Calculate the number of batches

        return IntStream.range(0, numBatches)
                .mapToObj(i -> productIds.subList(i * batchSize, Math.min(totalSize, (i + 1) * batchSize)))
                .collect(Collectors.toList());
    }
}
