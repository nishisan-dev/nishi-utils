package dev.nishisan.utils.oss.engine;

import dev.nishisan.utils.oss.api.LateSampleAction;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.definition.LateSamplePolicy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LateSampleHandlerTest {

    private static final long BLOCK_START = 1_000_000L;
    private static final long BLOCK_SIZE = 21_600_000L; // 6h em ms

    @Test
    void amostraDentroDoBlocoRetornaInTime() {
        Sample s = new Sample(BLOCK_START + 5_000L, 10.0);
        LateSampleHandler.Outcome out = LateSampleHandler.classify(s, BLOCK_START, BLOCK_SIZE,
                new LateSamplePolicy(600, LateSampleAction.BUCKET_IF_POSSIBLE));
        assertEquals(LateSampleHandler.Outcome.IN_TIME, out);
    }

    @Test
    void amostraAposBlocoRetornaFuture() {
        Sample s = new Sample(BLOCK_START + BLOCK_SIZE + 1L, 10.0);
        LateSampleHandler.Outcome out = LateSampleHandler.classify(s, BLOCK_START, BLOCK_SIZE,
                new LateSamplePolicy(600, LateSampleAction.BUCKET_IF_POSSIBLE));
        assertEquals(LateSampleHandler.Outcome.FUTURE, out);
    }

    @Test
    void amostraAtrasadaDentroDaTolerancia() {
        Sample s = new Sample(BLOCK_START - 60_000L, 10.0); // 1min antes
        LateSampleHandler.Outcome out = LateSampleHandler.classify(s, BLOCK_START, BLOCK_SIZE,
                new LateSamplePolicy(600, LateSampleAction.BUCKET_IF_POSSIBLE));
        assertEquals(LateSampleHandler.Outcome.BUCKETED_LATE, out);
    }

    @Test
    void amostraAtrasadaForaDaToleranciaCai() {
        Sample s = new Sample(BLOCK_START - 700_000L, 10.0); // 11.6min antes
        LateSampleHandler.Outcome out = LateSampleHandler.classify(s, BLOCK_START, BLOCK_SIZE,
                new LateSamplePolicy(600, LateSampleAction.BUCKET_IF_POSSIBLE));
        assertEquals(LateSampleHandler.Outcome.DROPPED, out);
    }

    @Test
    void politicaLateDropDescartaMesmoDentroDaTolerancia() {
        Sample s = new Sample(BLOCK_START - 60_000L, 10.0);
        LateSampleHandler.Outcome out = LateSampleHandler.classify(s, BLOCK_START, BLOCK_SIZE,
                new LateSamplePolicy(600, LateSampleAction.LATE_DROP));
        assertEquals(LateSampleHandler.Outcome.DROPPED, out);
    }
}
