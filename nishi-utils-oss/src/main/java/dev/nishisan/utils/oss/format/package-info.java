/**
 * Formato binário e serialização de manifestos do ngrrd.
 *
 * <p>Reúne o codec dos blocos de payload (<strong>NGRD</strong>: header + array de
 * doubles + CRC32) e o codec YAML do manifesto versionado que liga seriesKey
 * a blocos persistidos no Storage.</p>
 *
 * <p>Layout binário do bloco (sem timestamps no payload — derivados de
 * {@code blockStartEpoch + i*stepSec}):</p>
 *
 * <pre>
 * +----------------+--------+-----------+-------+----+----+--------------------+------+---------+
 * | MAGIC (4)      | VER(2) | FLAGS(2)  | STEP  | CF | DS | BLOCK_START_EPOCH  | ROWS | CRC32   |
 * | 0x4E 47 52 44  |        |           | (4)   |(1) |(1) |       (8)          | (4)  | (4)     |
 * +----------------+--------+-----------+-------+----+----+--------------------+------+---------+
 * | PAYLOAD: ROWS x 8 bytes (double IEEE-754; NaN = unknown/missing)                            |
 * +--------------------------------------------------------------------------------------------+
 * </pre>
 */
package dev.nishisan.utils.oss.format;
