package group.aelysium.ara;


import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Flux exist to abstract away the Particle instance from the rest of your code.
 * This allows particles to exist in super-position, where they can start, stop, and reboot at will.
 * @param <P> The underlying Particle that exists within this flux.
 */
public class Flux<P extends Closure> implements Closure {
    private final List<Consumer<P>> onStart = new Vector<>();
    private final List<Runnable> onClose = new Vector<>();
    private final Map<String, Object> metadata = new ConcurrentHashMap<>();
    private final AtomicReference<Status> status = new AtomicReference<>(Status.INACTIVE);
    private final AtomicReference<@NotNull CompletableFuture<P>> resolvable = new AtomicReference<>(new CompletableFuture<>());
    private final AtomicReference<@NotNull Supplier<P>> builder;
    
    protected Flux(@NotNull Supplier<P> builder) {
        this.builder = new AtomicReference<>(builder);
    }
    
    public Status status() {
        return this.status.get();
    }
    
    /**
     * Returns the Flux as an Optional.
     * If the internal Particle exists, the Optional will contain it.
     * Otherwise, the Optional will be empty.
     * @return An Optional possibly containing the Particle.
     */
    public @NotNull Optional<P> asOptional() {
        CompletableFuture<P> future = this.resolvable.get();
        return Optional.ofNullable(future.getNow(null));
    }
    
    /**
     * Returns the Flux as a CompletableFuture.
     * If the internal Particle is actively running, the future will resolve instantly.
     * If the internal Particle is in the process of booting, the future will lock until the booting process is finished.
     * If the internal Particle hasn't been initialized, the future will be null.
     * @return A CompletableFuture or null representing the state of the internal Particle.
     */
    public @Nullable CompletableFuture<P> asFuture() {
        return this.resolvable.get();
    }
    
    public @NotNull P get() throws NullPointerException {
        CompletableFuture<P> future = this.resolvable.get();
        return future.getNow(null);
    }
    public @NotNull P get(int timeout, TimeUnit unit) throws ExecutionException, InterruptedException, TimeoutException {
        CompletableFuture<P> future = this.resolvable.get();
        return future.get(timeout, unit);
    }
    
    /**
     * Adds the provided metadata to the flux.
     * @param key The key for the data to store.
     * @param value The value to store.
     */
    public void metadata(String key, Object value) {
        this.metadata.put(key, value);
    }
    
    /**
     * Gets a metadata value from this flux.
     * Metadata may change if the tinder backing this flux changes as well.
     * @param key The key to search for.
     * @return The value associated with the key.
     * @param <T> The type that the value should be cast to.
     * @throws ClassCastException If the type of the value doesn't match the type being enforced by the generic.
     */
    public <T> @Nullable T metadata(String key) throws ClassCastException {
        return (T) this.metadata.get(key);
    }
    
    /**
     * Returns the full collection of metadata.
     * The returned map is immutable.
     * @return A map containing all metadata for the flux.
     */
    public @NotNull Map<String, Object> metadata() {
        return Collections.unmodifiableMap(this.metadata);
    }
    
    /**
     * Returns the underlying Particle is it exists, or throws an exception if it doesn't.
     * @return The underlying Particle.
     * @throws NoSuchElementException If no Particle exists.
     */
    public P orElseThrow() throws NoSuchElementException {
        return this.asOptional().orElseThrow();
    }
    
    /**
     * Returns the underlying Particle is it exists, or throws an exception if it doesn't.
     * @return The underlying Particle.
     * @throws NoSuchElementException If no Particle exists.
     */
    public <E extends Throwable> P orElseThrow(Supplier<E> exceptionResolver) throws E {
        return this.asOptional().orElseThrow(exceptionResolver);
    }
    
    /**
     * Returns the underlying Particle is it exists, or throws an exception if it doesn't.
     * @return The underlying Particle.
     * @throws NoSuchElementException If no Particle exists.
     */
    public <E extends Throwable> P orElse(@Nullable P other) {
        return this.asOptional().orElse(other);
    }
    
    /**
     * Checks if the Particle exists.
     * If this returns true, you should be able to instantly access the Particle.
     * @return `true` if the Particle exists. `false` otherwise.
     */
    public boolean isPresent() {
        CompletableFuture<P> future = this.resolvable.get();
        if(future == null) return false;
        if(future.isCancelled()) return false;
        if(future.isCompletedExceptionally()) return false;
        return future.isDone();
    }
    
    /**
     * Validates that the underlying object instance isn't available.
     * Specifically ensures that {@link #isPresent()} == false.
     * @return `true` if {@link #isPresent()} is false. `false` otherwise.
     */
    public boolean isEmpty() {
        return !this.isPresent();
    }
    
    /**
     * Performs an operation immediately if the object instance is available.
     * Otherwise, does nothing.
     * @param ifPresent The consumer to execute if the underlying object exists.
     */
    public void ifPresent(@NotNull Consumer<P> ifPresent) {
        this.compute(ifPresent, null, 0, TimeUnit.SECONDS);
    }
    
    /**
     * Performs an operation immediately if the object instance is not available.
     * Otherwise, does nothing.
     * @param ifEmpty The runnable to execute if the underlying object does not exist.
     */
    public void ifAbsent(@NotNull Runnable ifEmpty) {
        this.compute(null, ifEmpty, 0, TimeUnit.SECONDS);
    }
    
    /**
     * Performs an operation immediately if the object instance either is or is not available.
     * @param ifPresent The consumer to execute if the underlying object exists.
     * @param ifEmpty The runnable to execute if the underlying object does not exist.
     */
    public void compute(@NotNull Consumer<P> ifPresent, @NotNull Runnable ifEmpty) {
        this.compute(ifPresent, ifEmpty, 0, TimeUnit.SECONDS);
    }
    
    /**
     * Performs an operation if the object instance is available after some amount of time.
     * @param ifPresent The consumer to execute if the underlying object exists.
     * @param amount The amount of time to wait before performing the evaluation.
     * @param unit The time unit to apply to `amount`.
     */
    public void compute(@NotNull Consumer<P> ifPresent, int amount, @NotNull TimeUnit unit) {
        this.compute(ifPresent, null, amount, unit);
    }
    
    /**
     * Performs an operation if the object instance either is or is not available after some amount of time.
     * @param ifPresent The consumer to execute if the underlying object exists.
     * @param ifEmpty The runnable to execute if the underlying object does not exist.
     * @param amount The amount of time to wait before performing the evaluation.
     * @param unit The time unit to apply to `amount`.
     */
    public void compute(@Nullable Consumer<P> ifPresent, @Nullable Runnable ifEmpty, int amount, @NotNull TimeUnit unit) {
        if(this.isPresent()) {
            Optional<P> p = Optional.empty();
            try {
                if(amount == 0) p = Optional.ofNullable(this.resolvable.get().getNow(null));
                else p = Optional.ofNullable(this.resolvable.get().get(amount, unit));
            } catch (Exception ignore) {}
            
            if(p.isPresent()) {
                if(ifPresent == null) return;
                ifPresent.accept(p.orElseThrow());
                return;
            }
        }
        
        if(ifEmpty == null) return;
        ifEmpty.run();
    }
    
    /**
     * Runs the specified consumer when a new instance of the particle is created.
     * If a particle already exists, this method will run instantly.
     * @param consumer The consumer to run.
     */
    public void onStart(@NotNull Consumer<P> consumer) {
        this.onStart.add(consumer);
        if(this.isPresent()) consumer.accept(this.orElseThrow());
    }
    
    /**
     * Runs the specified runnable when the particle closes.
     * If the particle is already closed, this method will run instantly.
     * @param runnable The runnable to execute when the particle closes.
     */
    public void onClose(@NotNull Runnable runnable) {
        this.onClose.add(runnable);
        if(this.isEmpty()) runnable.run();
    }
    
    private void build(@NotNull Supplier<P> builder) throws InterruptedException {
        if(this.isPresent()) throw new InterruptedException("You must close the already running Particle before building a new one.");
        
        CompletableFuture<P> future = new CompletableFuture<>();
        this.resolvable.set(future);
        this.status.set(Status.STARTING);
        
        P p = builder.get();
        
        if(p == null) {
            future.completeExceptionally(new RuntimeException("Particle failed to build."));
            this.status.set(Status.FAILED_BUILD);
            return;
        }
        
        this.status.set(Status.ACTIVE);
        future.complete(p);
        
        try {
            this.handleStart(p);
        } catch (Exception ignore) {}
    }
    
    /**
     * Attempts to build a new instance of the underlying object.
     * @throws InterruptedException If there is already an instance of the underlying object. You must {@link #close()} it first before building a new one.
     */
    public void build() throws InterruptedException {
        this.build(this.builder.get());
    }
    
    /**
     * Attempts to {@link #close()} and then {@link #build()}.
     */
    public void rebuild() throws Exception {
        this.close();
        this.build();
    }
    
    /**
     * Attempts to {@link #close()} and then {@link #build()} using the newly provided supplier.
     * If the build attempt fails and `rollback` is true, this method will fall back to the already existing Supplier.
     * @param newBuilder The new supplier to attempt to build from.
     * @param rollback Should this method rollback and attempt to build with the old supplier if the new one fails to build.
     * @return `true` if the rebuild operation completed successfully.
     */
    public boolean rebuild(@NotNull Supplier<P> newBuilder, boolean rollback) throws Exception {
        this.close();
        
        try {
            this.build(newBuilder);
            this.orElseThrow();
            this.builder.set(newBuilder);
            return true;
        } catch (Exception ignore) {
            this.close();
        }
        
        if(!rollback) return false;
        
        this.build();
        return true;
    }
    
    /**
     * Attempts to close the underlying object instance.
     * Once this method is run, the caller should be able to safely call {@link #build()} again if they want to rebuild the object.
     */
    public void close() {
        try {
            this.status.set(Status.INACTIVE);
            CompletableFuture<P> future = this.resolvable.get();
            if(future.isDone())
                try {
                    future.getNow(null).close();
                } catch (Exception ignore) {}
            else future.completeExceptionally(new InterruptedException("Particle boot was interrupted by its Flux closing!"));
            this.handleClose();
        } catch (Exception ignore) {}
    }
    
    private void handleStart(P particle) {
        this.onStart.forEach(c -> {
            try {
                c.accept(particle);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
    private void handleClose() {
        this.onClose.forEach(r -> {
            try {
                r.run();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
    
    public static <T extends Closure> @NotNull Flux<T> using(@NotNull Supplier<T> builder) {
        return new Flux<>(builder);
    }
    
    public enum Status {
        ACTIVE,
        STARTING,
        INACTIVE,
        FAILED_BUILD
    }
}