package group.aelysium.ara;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Particles are the backbone of the Absolute Redundancy Architecture.
 * By leveraging {@link Flux}, Particles are able to exist in a state of super-positioning.
 */
public interface Particle extends Closure {
    /**
     * Tinder is the configuration point for a {@link Particle}.
     * Via a {@link Flux}, a single Tinder must be deterministic of the {@link Particle} produced.
     * @param <P> The Particle type that will be launched via this Tinder.
     */
    abstract class Tinder<P extends Particle> {
        private final Map<String, Object> metadata = new HashMap<>();

        protected Tinder() {}

        /**
         * Adds the metadata to the tinder.
         * Once a tinder is created from the flux, this metadata will be passed to the flux and be available for reading.
         * @param key The key to store.
         * @param value The value to store.
         * @return `true` if the value could be stored. `false` if the specified key already exists.
         */
        public boolean metadata(String key, String value) {
            if(this.metadata.containsKey(key)) return false;
            this.metadata.put(key, value);
            return true;
        }

        protected Map<String, Object> metadata() {
            return Collections.unmodifiableMap(this.metadata);
        }

        /**
         * @return A new Flux containing the tinder.
         */
        public final Flux<P> flux() {
            return new Flux<>(this);
        }

        /**
         * Based on the contents of this Tinder, ignite a new Particle.
         * Only two results are acceptable, either a fully-functioning Particle is returned.
         * Or this method throws an exception.
         * @return A fully functional Particle.
         * @throws Exception If there is any issue at all constructing the microservice.
         */
        public abstract @NotNull P ignite() throws Exception;
    }

    /**
     * Flux exist to abstract away the Particle instance from the rest of your code.
     * This allows particles to exist in super-position, where they can start, stop, and reboot at will.
     * @param <P> The underlying Particle that exists within this flux.
     */
    class Flux<P extends Particle> implements Closure {
        private static final ExecutorService executor = Executors.newCachedThreadPool();
        private @Nullable List<Consumer<P>> onStart = null;
        private @Nullable List<Runnable> onClose = null;
        private @Nullable CompletableFuture<P> resolvable = null;
        private @NotNull Tinder<P> tinder;

        protected Flux(@NotNull Tinder<P> tinder) {
            this.tinder = tinder;
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
            return (T) this.tinder.metadata().get(key);
        }

        /**
         * Returns the full collection of metadata.
         * The returned map is immutable.
         * @return A map containing all metadata for the flux.
         */
        public @NotNull Map<String, Object> metadata() {
            return Collections.unmodifiableMap(this.tinder.metadata());
        }

        private void handleStart(P particle) {
            if(this.onStart == null) return;
            this.onStart.forEach(c -> {
                try {
                    c.accept(particle);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        private void handleClose() {
            if(this.onClose == null) return;
            this.onClose.forEach(r -> {
                try {
                    r.run();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        /**
         * Ignites a new Particle via the Tinder associated with this Flux and returns it.
         * @return A Future that will either resolve into the Particle or resolve exceptionally.
         */
        private CompletableFuture<P> ignite(@NotNull Tinder<P> tinder) {
            CompletableFuture<P> future = new CompletableFuture<>();

            executor.submit(() -> {
                try {
                    P p = tinder.ignite();

                    try {
                        this.handleStart(p);
                    } catch (Exception ignore) {}

                    future.complete(p);
                } catch (Exception e) {
                    future.completeExceptionally(e);
                }
            });

            return future;
        }

        /**
         * Re-ignites the Particle governed by this Flux.
         * This method will ignite a new instance of this particle using a new Tinder.
         * <br />
         * If this method properly ignites a new Particle with the new Tinder, this Flux will store the new Tinder and use it going forward.
         * @param tinder The tinder to ignite a new Particle with.
         * @param rollback If the passed Tinder fails to ignite. Should we attempt to ignite the old Tinder that already existed on this Flux?
         *                 If this setting is enabled, the current thread will lock until the Tinder either succeeds in ignition or until it fails Exceptionally.
         * @return A future that will either resolve into the Particle, or resolve exceptionally.
         * @throws Exception If there's an issue shutting down the current Particle (If one exists)
         */
        public CompletableFuture<P> reignite(@NotNull Tinder<P> tinder, boolean rollback) throws Exception {
            if(this.resolvable != null) {
                P particle = this.resolvable.getNow(null);
                if(particle == null) this.resolvable.cancel(true);
                else this.close();
            }

            this.resolvable = this.ignite(tinder);

            if(rollback)
                try {
                    this.resolvable.get();

                    // If succeeds, this is the new Tinder
                    this.tinder = tinder;
                } catch (Exception ignore) {
                    this.resolvable = this.ignite(this.tinder);
                }

            return this.resolvable;
        }

        public CompletableFuture<P> reignite(@NotNull Tinder<P> tinder) throws Exception {
            return this.reignite(tinder, false);
        }

        public CompletableFuture<P> reignite(boolean rollback) throws Exception {
            return this.reignite(this.tinder, rollback);
        }

        public CompletableFuture<P> reignite() throws Exception {
            return this.reignite(false);
        }

        /**
         * Returns the underlying Particle is it exists, or throws an exception if it doesn't.
         * @return The underlying Particle.
         * @throws NoSuchElementException If no Particle exists.
         */
        public P orElseThrow() throws NoSuchElementException {
            P p = this.access().getNow(null);
            if(p == null) throw new NoSuchElementException();
            return p;
        }

        /**
         * Returns the underlying Particle is it exists, or throws an exception if it doesn't.
         * @return The underlying Particle.
         * @throws NoSuchElementException If no Particle exists.
         */
        public <E extends Throwable> P orElseThrow(Supplier<E> exceptionResolver) throws E {
            P p = this.access().getNow(null);
            if(p == null) throw exceptionResolver.get();
            return p;
        }

        /**
         * Runs the Consumer using the Particle.
         * If the Particle doesn't exist, this method will store the consumer on a parallel thread and wait for the Particle to attempt ignition.
         * <br />
         * If the Particle is unable to ignite in under 1 minute, the consumer will be thrown away.
         * <br />
         * To set longer timeouts you can use {@link #executeParallel(Consumer, int, TimeUnit)}.
         * @param consumer The consumer to execute with the ignited Particle.
         */
        public void executeParallel(Consumer<P> consumer) {
            this.executeParallel(consumer, 1, TimeUnit.SECONDS);
        }

        /**
         * Runs the Consumer using the Particle.
         * If the Particle doesn't exist, this method will store the consumer on a parallel thread and wait for the Particle to attempt ignition.
         * <br />
         * If the Particle is unable to ignite in under 1 minute, the consumer will be thrown away.
         * @param consumer The consumer to execute with the ignited Particle.
         * @param amount The amount of time to give the Particle for ignition before giving up.
         * @param unit The time unit to use for "amount".
         */
        public void executeParallel(Consumer<P> consumer, int amount, TimeUnit unit) {
            if(this.resolvable == null) return;
            if(this.resolvable.isCancelled()) return;
            if(this.resolvable.isCompletedExceptionally()) return;
            if(this.exists()) {
                try {
                    consumer.accept(this.access().get(amount, unit));
                } catch (Exception ignore) {}
                return;
            }

            executor.submit(()->{
                try {
                    consumer.accept(this.access().get(amount, unit));
                } catch (Exception ignore) {}
            });
        }

        /**
         * Runs the Consumer using the Particle.
         * This method is not thread-locking and will always execute the Consumer instantly.
         * <br/>
         * This method respects Exceptions that may be thrown within the Consumer.
         * <br />
         * If the Particle is not available, the Consumer is thrown away.
         * @param success The consumer to execute if the Particle is available.
         */
        public void executeNow(Consumer<P> success) {
            this.executeNow(success, ()->{});
        }

        /**
         * Runs either Consumer or Runnable based on if the Particle is available.
         * This method is not thread-locking and will always execute either the Consumer or the Runnable instantly.
         * <br/>
         * This method respects Exceptions that may be thrown within the Consumer or Runnable.
         * Any exceptions that might be thrown will be passed along to the caller to handle.
         * @param success The consumer to execute if the Particle is available.
         * @param failed The Runnable if the Particle isn't available.
         */
        public void executeNow(Consumer<P> success, Runnable failed) {
            this.executeLocking(success, failed, 0, TimeUnit.SECONDS);
        }

        /**
         * Runs the Consumer using the Particle.
         * This method is thread locking for no longer than the duration of the timeout.
         * <br/>
         * If the Particle currently exists, it will resolve instantly. If the Particle doesn't exist, timeout will determine how
         * long this method will wait before running either the success Consumer or the failed Runnable.
         * <br/>
         * This method respects Exceptions that may be thrown within the Consumer.
         * Any exceptions that might be thrown will be passed along to the caller to handle.
         * @param success The consumer to execute if the Particle is available.
         * @param amount The amount of time to give the Particle to resolve before throwing away the Consumer.
         * @param unit The time unit to use for "amount".
         */
        public void executeLocking(Consumer<P> success, int amount, TimeUnit unit) {
            this.executeLocking(success, ()->{}, amount, unit);
        }

        /**
         * Executes either the Consumer or the Runnable based on if the Particle is available or not after the delay.
         * This method is thread locking for no longer than the duration of the timeout.
         * <br />
         * If the Particle currently exists, it will resolve instantly. If the Particle doesn't exist, timeout will determine how
         * long this method will wait before running either the success Consumer or the failed Runnable.
         * <br/>
         * This method respects Exceptions that may be thrown within the Consumer or Runnable.
         * Any exceptions that might be thrown will be passed along to the caller to handle.
         * @param success The consumer to execute if the Particle is available.
         * @param failed The Runnable if the Particle isn't available.
         * @param amount The amount of time to give the Particle to resolve before running the Runnable.
         * @param unit The time unit to use for "amount".
         */
        public void executeLocking(Consumer<P> success, Runnable failed, int amount, TimeUnit unit) {
            if(this.exists()) {
                Optional<P> p = Optional.empty();
                try {
                    if(amount == 0) p = Optional.ofNullable(this.access().getNow(null));
                    else p = Optional.ofNullable(this.access().get(amount, unit));
                } catch (Exception ignore) {}

                if(p.isPresent()) {
                    success.accept(p.orElseThrow());
                    return;
                }
            }

            failed.run();
        }

        /**
         * Access the underlying Particle.
         * Particles exist in a state of super-position, there's no way to know if a particle is currently active until you observe it.
         * This method is equivalent to calling {@link #access() .access()}{@link CompletableFuture#get() .get()}.
         * @return A Particle if it was able to ignite. If the Particle wasn't able to ignite, this method will throw an exception.
         * @throws Exception If the future completes exceptionally. i.e. the Particle failed to ignite.
         */
        public P observe() throws Exception {
            return this.access().get();
        }

        /**
         * Access the underlying Particle.
         * Particles exist in a state of super-position, there's no way to know if a particle is currently active until you observe it.
         * This method is equivalent to calling {@link #access() .access()}{@link CompletableFuture#get() .get()}.
         * @param timeout The amount to wait before timing out.
         * @param unit The time unit to use for "amount".
         * @return A Particle if it was able to ignite. If the Particle wasn't able to ignite, this method will throw an exception.
         * @throws Exception If the future completes exceptionally. i.e. the Particle failed to ignite.
         */
        public P observe(int timeout, TimeUnit unit) throws Exception {
            return this.access().get(timeout, unit);
        }

        /**
         * Access the Particle through it's CompletableFuture.
         * Particles exist in a state of super-position, there's no way to know if a Particle is currently active until you observe it.
         * <br />
         * If this Particle does not exist, this method will attempt to ignite a new instance of this Particle.
         * @return A future that will resolve to the finished Particle if it's able to boot. If the Particle wasn't able to boot, the future will complete exceptionally.
         */
        public CompletableFuture<P> access() {
            if(this.resolvable != null)
                return this.resolvable;

            this.resolvable = this.ignite(this.tinder);

            return this.resolvable;
        }

        /**
         * Checks if the Particle exists.
         * If this returns true, you should be able to instantly access the Particle.
         * @return `true` if the Particle exists. `false` otherwise.
         */
        public boolean exists() {
            if(this.resolvable == null) return false;
            if(this.resolvable.isCancelled()) return false;
            if(this.resolvable.isCompletedExceptionally()) return false;
            return this.resolvable.isDone();
        }

        /**
         * Runs the specified consumer when a new instance of the particle is created.
         * If a particle already exists, this method will run instantly.
         * @param consumer The consumer to run.
         */
        public void onStart(@NotNull Consumer<P> consumer) {
            if(this.onStart == null) this.onStart = new ArrayList<>();
            this.onStart.add(consumer);
            if(this.exists()) consumer.accept(this.orElseThrow());
        }

        /**
         * Runs the specified runnable when the particle closes.
         * If the particle is already closed, this method will run instantly.
         * @param runnable The runnable to execute when the particle closes.
         */
        public void onClose(@NotNull Runnable runnable) {
            if(this.onClose == null) this.onClose = new ArrayList<>();
            this.onClose.add(runnable);
            if(!this.exists()) runnable.run();
        }

        /**
         * Fetches the Tinder being used by this flux.
         * @return The Tinder.
         */
        public Tinder<P> tinder() {
            return this.tinder;
        }

        public void close() {
            if(this.resolvable == null) return;
            if(this.resolvable.isDone())
                try {
                    this.resolvable.get().close();
                } catch (Exception ignore) {}
            else this.resolvable.completeExceptionally(new InterruptedException("Particle boot was interrupted by its Flux closing!"));
            this.resolvable = null;
            this.handleClose();
        }

        /**
         * Implements {@link Object#equals(Object)} where the {@link Tinder} of the two Fluxes are compared.
         * @param o The Flux to compare with.
         * @return `true` or `false` based on the equality of the two Fluxes.
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Flux<?> flux = (Flux<?>) o;
            return Objects.equals(tinder, flux.tinder);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tinder);
        }
    }
}