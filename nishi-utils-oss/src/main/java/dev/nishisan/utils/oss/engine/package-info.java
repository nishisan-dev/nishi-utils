/**
 * Núcleo da engine ngrrd (sem IO): time-bucketing, derivação counter→rate,
 * consolidação de CDPs, tratamento de amostras atrasadas e seleção
 * <em>best-fit</em> de RRA.
 *
 * <p>Todas as classes deste pacote são puras: dependem apenas da definição
 * carregada do YAML e dos inputs imediatos. Não tocam arquivo, rede ou estado
 * global — o que torna o teste unitário trivial e prepara terreno para
 * paralelização posterior.</p>
 */
package dev.nishisan.utils.oss.engine;
