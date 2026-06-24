package dev.nishisan.utils.oss;

import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;

import java.util.Map;

/**
 * Contrato público de operação de um pipeline ngrrd já configurado. Devolvido
 * por {@link Ngrrd#fromYaml(java.nio.file.Path,
 * dev.nishisan.utils.oss.storage.StorageFactory.StorageBindings, java.util.Map)}.
 *
 * <p>O escopo é o da série identificada pelas {@code tags} fornecidas — o
 * handle resolve {@code seriesKey} via {@code IdentitySpec.seriesKeyTemplate}.</p>
 *
 * <h2>Concorrência</h2>
 *
 * <p>O handle é seguro para <strong>1 writer lógico + N readers</strong> no
 * mesmo processo, sem serialização externa:</p>
 *
 * <ul>
 *   <li>{@link #read(String, ViewQuery)} (e variantes) pode ser chamado por N
 *       threads concorrentes entre si e com {@link #write(String, Sample)} /
 *       {@link #checkpoint()}. Cada leitura observa um estado consistente da
 *       série (ponteiros do ring e células sempre coerentes entre si); leituras
 *       não bloqueiam umas às outras.</li>
 *   <li>{@link #write(String, Sample)} é thread-safe (enfileira para a worker
 *       thread única do writer), mas a série permanece single-writer lógico:
 *       se múltiplas threads escreverem, a ordem relativa entre elas é
 *       indefinida — o que importa para counters/derives é a ordem temporal
 *       das amostras.</li>
 *   <li>{@link #checkpoint()} / {@link #flush()} são síncronos: drenam a fila
 *       do writer antes de retornar.</li>
 * </ul>
 *
 * <p><strong>Visibilidade por backend:</strong> no disco local, CDPs de passos
 * já fechados tornam-se legíveis assim que o passo fecha; o CDP em progresso
 * (parcial) torna-se legível após {@link #checkpoint()}. No S3, toda leitura
 * reflete o último {@link #checkpoint()} publicado (PUT atômico) — nada fica
 * visível entre checkpoints.</p>
 *
 * <p><strong>Fora do contrato:</strong> ler ou escrever a mesma série por um
 * segundo handle ou processo concorrente ao writer. O objeto {@code .ngrr}
 * sofre escrita in-place no disco (um segundo processo leitor pode observar
 * estado rasgado) e read-modify-write no S3 (dois writers causam perda
 * silenciosa, last-write-wins). O invariante "um writer por série" permanece;
 * todo acesso concorrente deve passar pelo mesmo handle.</p>
 *
 * <p>{@link #close()} faz shutdown ordenado: materializa o CDP em progresso,
 * torna o objeto da série durável e encerra a thread do writer. O handle não
 * deve ser usado após {@code close()}.</p>
 */
public interface NgrrdHandle extends AutoCloseable {

    /** chave da série utilizada por este handle. */
    String seriesKey();

    /** Enfileira uma amostra para o DS raw indicado. */
    void write(String dsName, Sample sample);

    /** Equivalente a {@link #checkpoint()} no formato de série única. */
    void flush();

    /**
     * Materializa o CDP em progresso como parcial e torna o objeto da série
     * durável (fsync no disco / PUT no S3), mantendo o estado vivo. Semântica
     * rrdtool-like: o dado fica legível antes do passo do RRA fechar.
     *
     * <p>É um no-op de durabilidade quando nenhuma amostra foi aplicada desde o
     * último force (idle-skip): a série já está durável naquele estado, então a
     * re-emissão parcial (byte-idêntica) e o {@code fsync}/{@code PUT} são
     * pulados. Não há perda de visibilidade — o slot em progresso já reflete o
     * último estado materializado.</p>
     */
    void checkpoint();

    /** Lê uma série materializada com base em uma {@link ViewQuery} explícita. */
    SeriesResult read(String dsName, ViewQuery query);

    /** Executa um preset declarativo, retornando uma série por DS listado. */
    Map<String, SeriesResult> read(String presetName);

    /**
     * Variante de {@link #read(String)} com {@code endExclusiveEpochMs}
     * explícito — útil para testes/replays determinísticos onde
     * {@link System#currentTimeMillis()} não reflete o range desejado.
     */
    Map<String, SeriesResult> read(String presetName, long endExclusiveEpochMs);

    /**
     * Variante de {@link #read(String, ViewQuery)} com {@code endExclusiveEpochMs}
     * explícito.
     */
    SeriesResult read(String dsName, ViewQuery query, long endExclusiveEpochMs);

    @Override
    void close();
}
