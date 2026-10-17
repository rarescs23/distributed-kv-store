package ro.distributed.kv.storage;

import java.util.Optional;

/**
 * Interfata contractuala pentru motorul de stocare local al fiecarui nod.
 */
public interface StorageEngine {

    /**
     * Salveaza sau suprascrie valoarea asociata cheii.
     *
     * @param key cheia
     * @param value valoarea versionata
     */
    void put(String key, VersionedValue value);

    /**
     * Salveaza valoarea doar daca este cel putin la fel de noua (LWW) ca cea existenta.
     * Operatia este atomica per cheie.
     *
     * @return true daca valoarea a fost aplicata, false daca exista deja una mai noua
     */
    boolean putIfNewer(String key, VersionedValue value);

    /**
     * Obtine valoarea versionata asociata cheii.
     *
     * @param key cheia cautata
     * @return Optional continand valoarea sau Optional.empty() daca nu exista
     */
    Optional<VersionedValue> get(String key);

    /**
     * Sterge cheia si valoarea asociata.
     *
     * @param key cheia de sters
     * @return true daca a fost stearsa, false daca nu exista
     */
    boolean delete(String key);

    /**
     * Verifica daca o cheie este stocata.
     *
     * @param key cheia verificata
     * @return true daca exista
     */
    boolean containsKey(String key);

    /**
     * Returneaza numarul total de chei stocate local.
     */
    int size();

    /**
     * Goleste toate datele stocate.
     */
    void clear();
}
