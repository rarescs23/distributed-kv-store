# Plan de Implementare - Faza 1: Core Hashing & Local Storage

Acest plan detaliază pașii tehnici pentru inițializarea proiectului Java și implementarea nucleului de **Consistent Hashing** și **Stocare Locală** (Faza 1 din arhitectura clusterului).

---

## Ghid de Verificare & Decizii pentru Utilizator

> [!NOTE]
> Vom folosi **Java 21** și **Maven 3.9+** (deja instalate și verificate pe mașină). Pentru hashing vom folosi **MurmurHash3 (128-bit trunchiat la 64-bit)** prin `com.google.guava`, oferind viteză ridicată și efect optim de avalanșă.

---

## Modificări Propuse

### 1. Configurare Proiect (Maven)

#### [NEW] [pom.xml](file:///Users/rarescusma/Desktop/Pr/pom.xml)
- Configurare Java 21 (`source`/`target` 21).
- Dependențe:
  - `com.google.guava:guava` (pentru MurmurHash3 și utilitare de concurență).
  - `org.slf4j:slf4j-api` și `ch.qos.logback:logback-classic` (logging structurat).
  - `org.junit.jupiter:junit-jupiter` & `org.assertj:assertj-core` (framework de testare).
  - Pregătire structură pentru dependințele gRPC din Faza 2.

---

### 2. Pachetul `ro.distributed.kv.core` (Consistent Hashing Ring)

#### [NEW] [HashFunction.java](file:///Users/rarescusma/Desktop/Pr/src/main/java/ro/distributed/kv/core/HashFunction.java)
- Interfață simplă: `long hash(String key);`

#### [NEW] [Murmur3Hash.java](file:///Users/rarescusma/Desktop/Pr/src/main/java/ro/distributed/kv/core/Murmur3Hash.java)
- Implementare a `HashFunction` bazată pe MurmurHash3 pe 64 de biți.

#### [NEW] [Node.java](file:///Users/rarescusma/Desktop/Pr/src/main/java/ro/distributed/kv/core/Node.java)
- Reprezentarea unui nod fizic din cluster (`id`, `host`, `port`). Include `equals` și `hashCode` pe baza `id`.

#### [NEW] [VirtualNode.java](file:///Users/rarescusma/Desktop/Pr/src/main/java/ro/distributed/kv/core/VirtualNode.java)
- Punct virtual pe inel: referință către `physicalNode`, indexul vnode-ului și cheia unică de hash (`nodeId + "#vnode-" + index`).

#### [NEW] [ConsistentHashRing.java](file:///Users/rarescusma/Desktop/Pr/src/main/java/ro/distributed/kv/core/ConsistentHashRing.java)
- Inelul de hashing bazat pe `java.util.TreeMap<Long, VirtualNode>` protejat de un `ReentrantReadWriteLock` pentru performanță la citiri concurente:
  - `addNode(Node node)`: generează `V` vnodes (implicit 150) și le plasează pe inel.
  - `removeNode(Node node)`: curăță toate vnodes-urile asociate.
  - `getNode(String key)`: returnează nodul fizic primar responsabil.
  - `getPreferenceList(String key, int n)`: parcurge inelul în sens orar începând de la `hash(key)`, colectând primele `n` **noduri fizice distincte** (ignoră vnodes duplicate) și gestionând circularitatea (**wrap-around** la `ring.firstKey()` când atinge capătul).

---

### 3. Pachetul `ro.distributed.kv.storage` (Stocare Locală)

#### [NEW] [VersionedValue.java](file:///Users/rarescusma/Desktop/Pr/src/main/java/ro/distributed/kv/storage/VersionedValue.java)
- `record VersionedValue(byte[] data, long timestamp, long version)`: suport pentru Last-Write-Wins (LWW) și rezoluție de conflicte.

#### [NEW] [StorageEngine.java](file:///Users/rarescusma/Desktop/Pr/src/main/java/ro/distributed/kv/storage/StorageEngine.java)
- Interfață: `put(String key, VersionedValue value)`, `get(String key)`, `delete(String key)`, `containsKey(String key)`.

#### [NEW] [MemoryStorageEngine.java](file:///Users/rarescusma/Desktop/Pr/src/main/java/ro/distributed/kv/storage/MemoryStorageEngine.java)
- Implementare thread-safe de stocare în memorie bazată pe `ConcurrentHashMap<String, VersionedValue>`.

---

### 4. Pachetul `test` (Suita de Testare)

#### [NEW] [Murmur3HashTest.java](file:///Users/rarescusma/Desktop/Pr/src/test/java/ro/distributed/kv/core/Murmur3HashTest.java)
- Testează determinismul și efectul de avalanșă.

#### [NEW] [ConsistentHashRingTest.java](file:///Users/rarescusma/Desktop/Pr/src/test/java/ro/distributed/kv/core/ConsistentHashRingTest.java)
- **Test Wrap-around:** Verifică cheile al căror hash depășește cel mai mare hash de nod.
- **Test Deduplicare Replici:** Verifică că `getPreferenceList(key, 3)` returnează 3 noduri fizice unice, chiar dacă întâlnește vnodes multiple ale aceluiași nod.
- **Test Adăugare/Eliminare Dinamică:** Verifică că la adăugarea/ștergerea unui nod, doar un subset minim de chei își schimbă atribuirea.
- **Test Dispersie Uniformă:** Inserează 100.000 de chei pe un cluster de 5 noduri și validează că deviația standard este sub 15%.

#### [NEW] [MemoryStorageEngineTest.java](file:///Users/rarescusma/Desktop/Pr/src/test/java/ro/distributed/kv/storage/MemoryStorageEngineTest.java)
- Testează operațiile CRUD și accesul concurent din multiple thread-uri.

---

## Plan de Verificare

### Teste Automate
- Rulare compilare Maven și teste unitare:
  ```bash
  mvn clean test
  ```
- Toate testele trebuie să treacă cu cod de ieșire `0`.
- Validarea logurilor pentru calculul deviației standard a distribuției de chei.
