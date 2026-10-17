package ro.distributed.kv.core;

/**
 * Interfata functionala pentru functiile de hash utilizate pe inel.
 */
@FunctionalInterface
public interface HashFunction {

    /**
     * Calculeaza o valoare hash pe 64 de biti (intreg cu semn) pentru o cheie data.
     *
     * @param key cheia sau identificatorul de hashuit
     * @return hash pe 64 de biti
     */
    long hash(String key);
}
