package dev.nishisan.utils.oss.config;

import dev.nishisan.utils.oss.api.DataSourceType;
import dev.nishisan.utils.oss.definition.ArchiveSpec;
import dev.nishisan.utils.oss.definition.DataSourceDef;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import dev.nishisan.utils.oss.definition.NgrrdSpec;
import dev.nishisan.utils.oss.definition.PresetDef;
import dev.nishisan.utils.oss.definition.RraDef;
import dev.nishisan.utils.oss.definition.TimeSpec;
import dev.nishisan.utils.oss.definition.ViewSpec;

import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Validações semânticas aplicadas após o parse YAML.
 *
 * <p>Conjunto canônico de checks (alinhado ao plano em
 * {@code planning/ngrrd-design.md}):</p>
 *
 * <ul>
 *     <li>apiVersion e kind compatíveis.</li>
 *     <li>{@code time.baseStepSec > 0} e divisível por 60.</li>
 *     <li>{@code time.blockSizeSec} múltiplo de {@code baseStepSec} e
 *     {@code >= max(rra.stepSec)}.</li>
 *     <li>Cada {@code rra.stepSec} múltiplo de {@code baseStepSec};
 *     {@code rows > 0}; {@code xff in [0.0, 1.0)}; {@code cf} não vazio.</li>
 *     <li>{@code archives.appliesTo.include} referencia DS existentes (raw ou
 *     derivado).</li>
 *     <li>{@code views.presets[].targetStepSec} resolvível por algum RRA.</li>
 *     <li>{@code identity.seriesKeyTemplate} usa apenas tags declaradas.</li>
 *     <li>Counters com {@code derive} têm {@code formula} usando apenas
 *     {@code delta}, {@code deltaT}, literais e operadores {@code + - * /}.</li>
 * </ul>
 */
public final class NgrrdDefinitionValidator {

    private static final Pattern FORMULA_TOKEN = Pattern.compile(
            "\\s*(deltaT|delta|[0-9]+(?:\\.[0-9]+)?|[()+\\-*/])\\s*");
    private static final Pattern TEMPLATE_PLACEHOLDER = Pattern.compile("\\{([A-Za-z0-9_]+)\\}");

    private NgrrdDefinitionValidator() {
    }

    public static void validate(NgrrdDefinition def) {
        if (def == null) {
            throw new NgrrdDefinitionException("Definição ausente");
        }
        validateRoot(def);
        NgrrdSpec spec = def.spec();
        validateTime(spec.time(), spec.archives());
        Set<String> dsNames = collectDataSourceNames(spec.dataSources());
        validateDataSources(spec.dataSources());
        validateArchives(spec.archives(), spec.time(), dsNames);
        validateViews(spec.views(), spec.archives());
        validateIdentity(spec.identity() == null ? null : spec.identity().seriesKeyTemplate(),
                spec.identity() == null ? List.of() : tagNames(spec.identity().tags()));
    }

    private static void validateRoot(NgrrdDefinition def) {
        String apiVersion = def.apiVersion();
        if (apiVersion == null || !apiVersion.startsWith("ngrrd/")) {
            throw new NgrrdDefinitionException("apiVersion deve começar com 'ngrrd/' (recebido: " + apiVersion + ")");
        }
        if (def.kind() == null || !def.kind().equals("MetricSeriesDefinition")) {
            throw new NgrrdDefinitionException("kind deve ser 'MetricSeriesDefinition' (recebido: " + def.kind() + ")");
        }
        if (def.metadata().name() == null || def.metadata().name().isBlank()) {
            throw new NgrrdDefinitionException("metadata.name é obrigatório");
        }
    }

    private static void validateTime(TimeSpec time, ArchiveSpec archives) {
        if (time == null) {
            throw new NgrrdDefinitionException("spec.time é obrigatório");
        }
        if (time.baseStepSec() <= 0) {
            throw new NgrrdDefinitionException("time.baseStepSec deve ser > 0");
        }
        if (time.blockSizeSec() <= 0) {
            throw new NgrrdDefinitionException("time.blockSizeSec deve ser > 0");
        }
        if (time.blockSizeSec() % time.baseStepSec() != 0) {
            throw new NgrrdDefinitionException(
                    "time.blockSizeSec (" + time.blockSizeSec() + ") deve ser múltiplo de baseStepSec ("
                            + time.baseStepSec() + ")");
        }
        if (archives != null) {
            int maxRraStep = archives.rras().stream().mapToInt(RraDef::stepSec).max().orElse(0);
            if (maxRraStep > time.blockSizeSec()) {
                throw new NgrrdDefinitionException(
                        "time.blockSizeSec (" + time.blockSizeSec() + ") deve ser >= max(rra.stepSec) ("
                                + maxRraStep + ")");
            }
        }
        if (time.lateSamplePolicy() != null && time.lateSamplePolicy().maxLatenessSec() < 0) {
            throw new NgrrdDefinitionException("lateSamplePolicy.maxLatenessSec não pode ser negativo");
        }
    }

    private static Set<String> collectDataSourceNames(List<DataSourceDef> dataSources) {
        Set<String> names = new LinkedHashSet<>();
        for (DataSourceDef ds : dataSources) {
            if (ds.name() == null || ds.name().isBlank()) {
                throw new NgrrdDefinitionException("dataSources[].name é obrigatório");
            }
            if (!names.add(ds.name())) {
                throw new NgrrdDefinitionException("dataSources[].name duplicado: " + ds.name());
            }
            if (ds.derive() != null && ds.derive().output() != null) {
                String derivedName = ds.derive().output().name();
                if (derivedName == null || derivedName.isBlank()) {
                    throw new NgrrdDefinitionException("derive.output.name é obrigatório em " + ds.name());
                }
                if (!names.add(derivedName)) {
                    throw new NgrrdDefinitionException("derive.output.name colide com outro DS: " + derivedName);
                }
            }
        }
        return names;
    }

    private static void validateDataSources(List<DataSourceDef> dataSources) {
        if (dataSources.isEmpty()) {
            throw new NgrrdDefinitionException("spec.dataSources deve conter ao menos um DS");
        }
        for (DataSourceDef ds : dataSources) {
            if (ds.type() == null) {
                throw new NgrrdDefinitionException("dataSources[" + ds.name() + "].type é obrigatório");
            }
            if (ds.type() == DataSourceType.COUNTER) {
                if (ds.counterBits() == null || (ds.counterBits() != 32 && ds.counterBits() != 64)) {
                    throw new NgrrdDefinitionException(
                            "counterBits deve ser 32 ou 64 em DS COUNTER: " + ds.name());
                }
            }
            if (ds.heartbeatSec() <= 0) {
                throw new NgrrdDefinitionException("heartbeatSec deve ser > 0 em DS " + ds.name());
            }
            if (ds.min() != null && ds.max() != null && ds.min() >= ds.max()) {
                throw new NgrrdDefinitionException("min deve ser < max em DS " + ds.name());
            }
            if (ds.derive() != null && ds.derive().output() != null) {
                validateFormula(ds.derive().output().formula(), ds.name());
            }
        }
    }

    private static void validateFormula(String formula, String dsName) {
        if (formula == null || formula.isBlank()) {
            throw new NgrrdDefinitionException("derive.output.formula é obrigatório em " + dsName);
        }
        Matcher tokenizer = FORMULA_TOKEN.matcher(formula);
        int cursor = 0;
        while (cursor < formula.length()) {
            if (Character.isWhitespace(formula.charAt(cursor))) {
                cursor++;
                continue;
            }
            tokenizer.region(cursor, formula.length());
            if (!tokenizer.lookingAt()) {
                throw new NgrrdDefinitionException(
                        "Token inválido em derive.output.formula de " + dsName + " na posição " + cursor
                                + ": '" + formula.substring(cursor) + "'");
            }
            cursor = tokenizer.end();
        }
    }

    private static void validateArchives(ArchiveSpec archives, TimeSpec time, Set<String> dsNames) {
        if (archives == null) {
            throw new NgrrdDefinitionException("spec.archives é obrigatório");
        }
        if (archives.rras().isEmpty()) {
            throw new NgrrdDefinitionException("archives.rras deve conter ao menos um RRA");
        }
        Set<String> rraNames = new HashSet<>();
        for (RraDef rra : archives.rras()) {
            if (rra.name() == null || rra.name().isBlank()) {
                throw new NgrrdDefinitionException("rra.name é obrigatório");
            }
            if (!rraNames.add(rra.name())) {
                throw new NgrrdDefinitionException("rra.name duplicado: " + rra.name());
            }
            if (rra.stepSec() <= 0 || rra.stepSec() % time.baseStepSec() != 0) {
                throw new NgrrdDefinitionException(
                        "rra.stepSec (" + rra.stepSec() + ") deve ser múltiplo de baseStepSec ("
                                + time.baseStepSec() + ") em " + rra.name());
            }
            if (rra.rows() <= 0) {
                throw new NgrrdDefinitionException("rra.rows deve ser > 0 em " + rra.name());
            }
            if (rra.xff() < 0.0 || rra.xff() >= 1.0) {
                throw new NgrrdDefinitionException("rra.xff deve estar em [0.0, 1.0) em " + rra.name());
            }
            if (rra.cf().isEmpty()) {
                throw new NgrrdDefinitionException("rra.cf deve conter ao menos uma função em " + rra.name());
            }
        }
        if (archives.appliesTo() != null) {
            for (String included : archives.appliesTo().include()) {
                if (!dsNames.contains(included)) {
                    throw new NgrrdDefinitionException(
                            "archives.appliesTo.include referencia DS inexistente: " + included);
                }
            }
            for (String excluded : archives.appliesTo().exclude()) {
                if (!dsNames.contains(excluded)) {
                    throw new NgrrdDefinitionException(
                            "archives.appliesTo.exclude referencia DS inexistente: " + excluded);
                }
            }
        }
    }

    private static void validateViews(ViewSpec views, ArchiveSpec archives) {
        if (views == null) {
            return;
        }
        Set<Integer> rraStepSecs = new HashSet<>();
        for (RraDef rra : archives.rras()) {
            rraStepSecs.add(rra.stepSec());
        }
        for (PresetDef preset : views.presets()) {
            if (preset.name() == null || preset.name().isBlank()) {
                throw new NgrrdDefinitionException("preset.name é obrigatório");
            }
            if (preset.window() == null || preset.window().isBlank()) {
                throw new NgrrdDefinitionException("preset.window é obrigatório em " + preset.name());
            }
            parseWindow(preset.window(), preset.name());
            if (preset.targetStepSec() <= 0) {
                throw new NgrrdDefinitionException("preset.targetStepSec deve ser > 0 em " + preset.name());
            }
            boolean coverable = rraStepSecs.stream().anyMatch(step -> step <= preset.targetStepSec());
            if (!coverable) {
                throw new NgrrdDefinitionException(
                        "preset " + preset.name() + " não é resolvível: nenhum RRA com stepSec <= "
                                + preset.targetStepSec());
            }
        }
    }

    /**
     * Janelas de preset são interpretadas pelo mesmo caminho que o
     * {@code ViewExecutor} usa em runtime ({@link Duration#parse(CharSequence)}).
     * Formatos baseados em meses/anos ({@code P1M}, {@code P1Y}) <strong>não</strong>
     * são aceitos — use {@code P30D}, {@code P365D} ou equivalentes em dias/horas
     * para que o validator e o executor concordem sobre o número de segundos
     * representado.
     */
    private static long parseWindow(String window, String presetName) {
        try {
            return Duration.parse(window).getSeconds();
        } catch (Exception e) {
            throw new NgrrdDefinitionException(
                    "preset.window inválido em " + presetName
                            + " (esperado ISO-8601 com dias/horas: P1D, P7D, PT1H, ... — meses/anos não suportados): "
                            + window, e);
        }
    }

    private static void validateIdentity(String template, List<String> declaredTags) {
        if (template == null || template.isBlank()) {
            return;
        }
        Matcher matcher = TEMPLATE_PLACEHOLDER.matcher(template);
        while (matcher.find()) {
            String placeholder = matcher.group(1);
            if (!declaredTags.contains(placeholder)) {
                throw new NgrrdDefinitionException(
                        "seriesKeyTemplate referencia tag não declarada: " + placeholder);
            }
        }
    }

    private static List<String> tagNames(List<dev.nishisan.utils.oss.definition.IdentityTag> tags) {
        return tags.stream().map(dev.nishisan.utils.oss.definition.IdentityTag::name).toList();
    }
}
