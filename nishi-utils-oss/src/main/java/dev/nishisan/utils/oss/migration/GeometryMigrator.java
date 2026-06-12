package dev.nishisan.utils.oss.migration;

import dev.nishisan.utils.oss.engine.PrimaryDataPoint;
import dev.nishisan.utils.oss.format.SeriesFileCodec;
import dev.nishisan.utils.oss.format.SeriesGeometry;
import dev.nishisan.utils.oss.format.SeriesLiveState;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Migração estrutural exata de uma série: reescreve a imagem do objeto na nova
 * geometria preservando a história compatível, sem reamostragem.
 *
 * <p>Colunas são remapeadas por nome derivado e archives por {@code (rra, cf)}.
 * Para cada archive comum (mesmo {@code stepSec} e {@code rows}), as células do
 * ring são copiadas posição a posição para a coluna correspondente; colunas e
 * archives novos permanecem {@code NaN}. A live-state é remapeada preservando o
 * ponteiro do ring por archive comum, o {@code last_ds}/PDP por coluna e o
 * {@code cdp_prep} por {@code (archive, coluna)} — mantendo, além da história, a
 * continuidade da derivação.</p>
 *
 * <p>Pré-condição: a mudança deve ser estruturalmente migrável (mesmo step base
 * e archives comuns com geometria de ring idêntica). Caso contrário lança
 * {@link IllegalStateException} — o chamador valida via
 * {@link GeometryDiff#structurallyMigratable()} antes.</p>
 */
public final class GeometryMigrator {

    private GeometryMigrator() {
    }

    public static byte[] migrate(SeriesGeometry oldGeo, SeriesGeometry newGeo,
                                 byte[] oldImage, byte[] newHash, int newRevision) {
        if (oldGeo.baseStepSec() != newGeo.baseStepSec()) {
            throw new IllegalStateException(
                    "baseStep mudou (" + oldGeo.baseStepSec() + " -> " + newGeo.baseStepSec()
                            + "): migração estrutural não aplicável");
        }
        int newD = newGeo.columnCount();
        int newA = newGeo.archiveCount();

        // Mapeamento coluna nova -> antiga (por nome derivado); -1 = coluna nova.
        int[] colOldByNew = new int[newD];
        for (int j = 0; j < newD; j++) {
            colOldByNew[j] = oldGeo.columnIndex(newGeo.columns().get(j).derivedName());
        }
        // Mapeamento archive novo -> antigo (por rra+cf); -1 = archive novo.
        int[] archOldByNew = new int[newA];
        for (int x = 0; x < newA; x++) {
            SeriesGeometry.Archive na = newGeo.archives().get(x);
            int y = oldGeo.archiveIndex(na.rraName(), na.cf());
            if (y >= 0) {
                SeriesGeometry.Archive oa = oldGeo.archives().get(y);
                if (oa.stepSec() != na.stepSec() || oa.rows() != na.rows()) {
                    throw new IllegalStateException("archive " + na.rraName() + "/" + na.cf()
                            + " mudou de geometria de ring: migração estrutural não aplicável");
                }
            }
            archOldByNew[x] = y;
        }

        byte[] newImage = SeriesFileCodec.buildInitialImage(newGeo, newHash, newRevision);
        SeriesLiveState oldLive = SeriesFileCodec.decodeLiveState(oldGeo,
                slice(oldImage, oldGeo.liveStateOffset(), oldGeo.liveStateBytes()));
        SeriesLiveState newLive = remapLiveState(oldGeo, newGeo, oldLive, colOldByNew, archOldByNew);

        byte[] liveBytes = SeriesFileCodec.encodeLiveState(newGeo, newLive);
        System.arraycopy(liveBytes, 0, newImage, (int) newGeo.liveStateOffset(), liveBytes.length);

        copyRings(oldGeo, newGeo, oldImage, newImage, colOldByNew, archOldByNew);
        return newImage;
    }

    private static SeriesLiveState remapLiveState(SeriesGeometry oldGeo, SeriesGeometry newGeo,
                                                  SeriesLiveState oldLive, int[] colOldByNew,
                                                  int[] archOldByNew) {
        int newD = newGeo.columnCount();
        int newA = newGeo.archiveCount();
        SeriesLiveState newLive = new SeriesLiveState(newD, newA);
        newLive.lastUpEpochMs = oldLive.lastUpEpochMs;

        for (int j = 0; j < newD; j++) {
            int i = colOldByNew[j];
            if (i < 0) {
                continue; // coluna nova: estado inicial.
            }
            newLive.counterPrevValue[j] = oldLive.counterPrevValue[i];
            newLive.counterPrevTsMs[j] = oldLive.counterPrevTsMs[i];
            newLive.pdp[j] = PrimaryDataPoint.restore(oldLive.pdp[i].snapshot());
            newLive.pdpSlotSec[j] = oldLive.pdpSlotSec[i];
        }
        for (int x = 0; x < newA; x++) {
            int y = archOldByNew[x];
            if (y < 0) {
                continue; // archive novo: estado inicial.
            }
            newLive.curRow[x] = oldLive.curRow[y];
            newLive.curRowEpochSec[x] = oldLive.curRowEpochSec[y];
            for (int j = 0; j < newD; j++) {
                int i = colOldByNew[j];
                if (i < 0) {
                    continue;
                }
                int nidx = newLive.cdpIndex(x, j);
                int oidx = oldLive.cdpIndex(y, i);
                newLive.cdpPartial[nidx] = oldLive.cdpPartial[oidx];
                newLive.cdpFolded[nidx] = oldLive.cdpFolded[oidx];
                newLive.cdpMissing[nidx] = oldLive.cdpMissing[oidx];
            }
        }
        return newLive;
    }

    private static void copyRings(SeriesGeometry oldGeo, SeriesGeometry newGeo,
                                  byte[] oldImage, byte[] newImage,
                                  int[] colOldByNew, int[] archOldByNew) {
        ByteBuffer src = ByteBuffer.wrap(oldImage).order(ByteOrder.BIG_ENDIAN);
        ByteBuffer dst = ByteBuffer.wrap(newImage).order(ByteOrder.BIG_ENDIAN);
        int newD = newGeo.columnCount();
        for (int x = 0; x < newGeo.archiveCount(); x++) {
            int y = archOldByNew[x];
            if (y < 0) {
                continue; // archive novo: rings permanecem NaN.
            }
            int rows = newGeo.archives().get(x).rows();
            for (int row = 0; row < rows; row++) {
                for (int j = 0; j < newD; j++) {
                    int i = colOldByNew[j];
                    if (i < 0) {
                        continue; // coluna nova: célula permanece NaN.
                    }
                    double v = src.getDouble((int) oldGeo.cellOffset(y, row, i));
                    dst.putDouble((int) newGeo.cellOffset(x, row, j), v);
                }
            }
        }
    }

    private static byte[] slice(byte[] image, long offset, long len) {
        return Arrays.copyOfRange(image, (int) offset, (int) (offset + len));
    }
}
