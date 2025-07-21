package org.tron.program;

import static org.tron.protos.contract.Common.ResourceCode.ENERGY;

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.tron.common.storage.leveldb.LevelDbDataSourceImpl;
import org.tron.common.utils.Sha256Hash;
import org.tron.common.utils.StringUtil;
import org.tron.core.capsule.ContractCapsule;
import org.tron.core.store.CodeStore;
import org.tron.core.vm.Op;
import org.tron.protos.Protocol;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;

@Slf4j(topic = "app")
public class DBTools {

  private static int count;
  private static long[] size = new long[100];

  private static void doScanContract() {
    //    LevelDbDataSourceImpl codeStore = DbUtils.openDb("code");
    LevelDbDataSourceImpl codeStore = null;
    codeStore
        .iterator()
        .forEachRemaining(
            e -> {
              byte[] code = e.getValue();
              if (code != null) {
                if (code.length < 1000) {
                  size[0] += 1;
                } else if (code.length < 5000) {
                  size[1] += 1;
                } else if (code.length < 10000) {
                  size[2] += 1;
                } else if (code.length < 20000) {
                  size[3] += 1;
                } else if (code.length < 50000) {
                  size[4] += 1;
                } else if (code.length < 100000) {
                  size[5] += 1;
                } else {
                  size[6] += 1;
                }
              }
              count += 1;
              //      if (code != null && code.length > 100_000) {
              //        System.out.println(StringUtil.encode58Check(e.getKey()) + ": " +
              // code.length);
              //      }
              //      count += 1;
              //      if (count % 10000 == 0) {
              //        System.out.println("traversal done: " + count);
              //      }
            });
    System.out.println(Arrays.toString(size));
  }

  private static void doScanAccount() {
    System.out.println("Start to scan account");
    LevelDbDataSourceImpl accountStore = null;
    AtomicLong blockCount = new AtomicLong();
    AtomicLong accountCount = new AtomicLong();
    long weight = 5_291_891_772L;
    long limit = 90_000_000_000L;
    accountStore.forEach(
        e -> {
          try {
            Protocol.Account account = Protocol.Account.parseFrom(e.getValue());

            long frozenBalanceV1 =
                account.getAccountResource().getFrozenBalanceForEnergy().getFrozenBalance()
                    + account.getAccountResource().getAcquiredDelegatedFrozenBalanceForEnergy();

            long frozenBalanceV2 =
                account.getFrozenV2List().stream()
                        .filter(o -> o.getType() == ENERGY)
                        .mapToLong(Protocol.Account.FreezeV2::getAmount)
                        .sum()
                    + account.getAccountResource().getAcquiredDelegatedFrozenV2BalanceForEnergy();

            long energy = (long) ((frozenBalanceV1 + frozenBalanceV2) / 1e6 * limit / weight);

            long usage = account.getAccountResource().getEnergyUsage();
            long size = account.getAccountResource().getEnergyWindowSize();

            if (size != 0 && usage / size > energy / 14400) {
              accountCount.incrementAndGet();
              logger.info(StringUtil.encode58Check(e.getKey()));
            }

            blockCount.incrementAndGet();
            if (blockCount.get() % 10000 == 0) {
              System.out.println(
                  "traversal done: " + blockCount.get() + ", account count: " + accountCount.get());
            }
          } catch (InvalidProtocolBufferException ex) {
            ex.printStackTrace();
          }
        });
  }

  public static void doScanMultiValidate() {
    LevelDbDataSourceImpl codeStore = null;
    String regx = "600a85858585";
    codeStore
        .iterator()
        .forEachRemaining(
            e -> {
              byte[] code = codeStore.getData(e.getKey());
              String codeHex = Hex.toHexString(code);
              if (codeHex.contains(regx)) {
                System.out.println(StringUtil.encode58Check(e.getKey()));
              }
              count += 1;
              if (count % 10000 == 0) {
                System.out.println("traversal done: " + count);
              }
            });
  }

  public static void doScanTronFigurePoint() {
    LevelDbDataSourceImpl contractStore = null;
    LevelDbDataSourceImpl codeStore = null;
    String regx = ".*d38015(.{4,10})57600080fd5b50d28015(.{4,10})57600080fd.*";
    Set<String> emptySet = new HashSet<>();
    Set<String> create2Set = new HashSet<>();
    contractStore
        .iterator()
        .forEachRemaining(
            e -> {
              ContractCapsule contractCapsule = new ContractCapsule(e.getValue());
              String creation =
                  Hex.toHexString(contractCapsule.getInstance().getBytecode().toByteArray());
              byte[] code = codeStore.getData(e.getKey());
              String runtime = code == null ? "" : Hex.toHexString(code);
              //      if (!Pattern.matches(regx, creation) && !Pattern.matches(regx, runtime)) {
              //        error += 1;
              //        set.add(runtime);
              //      }
              if ("".equals(runtime)) emptySet.add(StringUtil.encode58Check(e.getKey()));
              if (!contractCapsule.getInstance().getTrxHash().isEmpty())
                create2Set.add(StringUtil.encode58Check(e.getKey()));
              count += 1;
              if (count % 10000 == 0) {
                System.out.println("traversal done: " + count);
              }
            });
    System.out.println("not matched all: " + count);
    System.out.println("empty set: " + emptySet.size());
    System.out.println("create2 set: " + create2Set.size());
    create2Set.forEach(logger::info);
  }

  public static void doRemoveFigurePoint(byte[] ops) {
    List<OpRD> opRDs = compile(ops, null);
    int offset = 0;
    Iterator<OpRD> it = opRDs.listIterator();
    for (int i = 0; i < opRDs.size(); i++) {
      OpRD op = opRDs.get(i);
      if (op.is(Op.CALLTOKENID)) {
        for (int j = 0; j < 20; j++) {
          opRDs.remove(i);
        }
        offset += 26;
      } else if (op.is(Op.JUMP) || op.is(Op.JUMPI)) {
        if (opRDs.get(i - 1).is(Op.PUSH2)) {
          OpRD preOp = opRDs.get(i - 1);
          preOp.opd = preOp.opd.subtract(BigInteger.valueOf(offset));
        }
        if (opRDs.get(i - 2).is(Op.PUSH2)) {
          OpRD preOp = opRDs.get(i - 2);
          preOp.opd = preOp.opd.subtract(BigInteger.valueOf(offset));
        }
      }
    }
    StringBuilder sb = new StringBuilder();
    for (OpRD opRD : opRDs) {
      sb.append(opRD.toCode());
    }
    System.out.println(sb);
  }

  public static void doScanSpecialOpCode(CodeStore codeStore, int targetCode) {
    //    LevelDbDataSourceImpl codeStore = null;
    codeStore
        .iterator()
        .forEachRemaining(
            e -> {
              byte[] code = e.getValue().getData();
              if (code != null) {
                String address = StringUtil.encode58Check(e.getKey());
                List<OpRD> opRDs = compile(code, null);
              }
              count += 1;
              if (count % 10000 == 0) {
                System.out.println("traversal done: " + count);
              }
            });
    List<Map.Entry<String, Integer>> list = new ArrayList<>();
    // 升序排序
    list.sort(Map.Entry.comparingByValue());
    int stipend = 0, two = 0, four = 0, five = 0, seven = 0, gas = 0, other = 0;
    int[] stipendCounts = new int[1000];
    int[] twoCounts = new int[1000];
    int[] fourCounts = new int[1000];
    int[] fiveCounts = new int[1000];
    int[] sevenCounts = new int[1000];
    int[] gasCounts = new int[1000];
    for (Map.Entry<String, Integer> e : list) {
      if (e.getKey().contains("-2300 ")) {
        stipend += 1;
        if (recordDistance(stipendCounts, e.getKey()) > 100) {
          System.out.println(e.getKey() + ": " + e.getValue());
        }
      } else if (e.getKey().contains("-20000 ")) {
        two += 1;
        if (recordDistance(twoCounts, e.getKey()) > 100) {
          System.out.println(e.getKey() + ": " + e.getValue());
        }
      } else if (e.getKey().contains("-40000 ")) {
        four += 1;
        if (recordDistance(twoCounts, e.getKey()) > 100) {
          System.out.println(e.getKey() + ": " + e.getValue());
        }
      } else if (e.getKey().contains("-50000 ")) {
        five += 1;
        if (recordDistance(fiveCounts, e.getKey()) > 100) {
          System.out.println(e.getKey() + ": " + e.getValue());
        }
      } else if (e.getKey().contains("-70000 ")) {
        seven += 1;
        if (recordDistance(sevenCounts, e.getKey()) > 100) {
          System.out.println(e.getKey() + ": " + e.getValue());
        }
      } else if (e.getKey().contains(" GAS ")) {
        gas += 1;
        if (recordDistance(gasCounts, e.getKey()) > 100) {
          System.out.println(e.getKey() + ": " + e.getValue());
        }
      } else {
        other += 1;
        System.out.println(e.getKey() + ": " + e.getValue());
      }
    }
    System.out.printf("%d %d %d %d %d %d %d%n", stipend, two, four, five, seven, gas, other);
    printRecords(stipendCounts, "2300");
    printRecords(twoCounts, "20000");
    printRecords(fourCounts, "40000");
    printRecords(fiveCounts, "50000");
    printRecords(sevenCounts, "70000");
    printRecords(gasCounts, "GAS");
  }

  public static void doScanSpecialOpCodeV2(CodeStore codeStore, int targetCode) {
    AtomicLong count = new AtomicLong();
    codeStore
        .iterator()
        .forEachRemaining(
            e -> {
              byte[] code = e.getValue().getData();
              if (code != null) {
                String address = StringUtil.encode58Check(e.getKey());
                List<OpRD> opRDs = compile(code, null);
                if (opRDs.stream().anyMatch(op -> op.is(targetCode))) {
                  System.out.println(address);
                }
                //                System.out.println(address);
                //                opRDs.forEach(System.out::println);
              }
              count.addAndGet(1);
              if (count.get() % 10000 == 0) {
                System.out.println("traversal done: " + count);
              }
            });
  }

  public static void doScanChainID() {
    LevelDbDataSourceImpl codeStore = null;
    Set<String> fpSet = new HashSet<>();
    String chainID = "1ebf88508a03865c71d452e25f4d51194196a1d22b6653dc";
    codeStore
        .iterator()
        .forEachRemaining(
            e -> {
              byte[] code = e.getValue();
              if (code != null) {
                AtomicBoolean isFound = new AtomicBoolean(false);
                AtomicBoolean containChainID = new AtomicBoolean(false);
                compile(
                    code,
                    opRDS -> {
                      if (opRDS.isEmpty()) {
                        return true;
                      }
                      OpRD opRD = opRDS.get(opRDS.size() - 1);
                      if (opRD.isPushOp()
                          && chainID.equals(Hex.toHexString(opRD.opd.toByteArray()))) {
                        isFound.set(true);
                      }
                      if (opRD.opr == Op.CHAINID) {
                        containChainID.set(true);
                      }
                      //          if (opRDS.size() > 20 &&  opRDS.get(opRDS.size() - 10).opr ==
                      // Op.CHAINID) {
                      //            String fp = buildFingerPoint(opRDS, opRDS.size() - 10);
                      //            if (!fpSet.contains(fp)) {
                      //              fpSet.add(fp);
                      //              System.out.println(StringUtil.encode58Check(e.getKey()) + ": "
                      // + fp);
                      //            }
                      //            System.out.println(StringUtil.encode58Check(e.getKey()) + ": " +
                      // fp);
                      //          }
                      return true;
                    });
                if (containChainID.get()) {
                  System.out.println(isFound.get());
                }
              }
              //      count += 1;
              //      if (count % 10000 == 0) {
              //        System.out.println("traversal done: " + count);
              //      }
            });
  }

  private static String buildFingerPoint(List<OpRD> ops, int index) {
    int count = 20;
    index -= count / 2;
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < count; i++) {
      sb.append(ops.get(index + i)).append(" ");
    }
    return sb.toString();
  }

  public static void doScanShieldMethods() {
    BigInteger verifyMintProof = new BigInteger(Hex.decode("01000001"));
    Set<String> verifyMintProofSet = new HashSet<>();
    BigInteger verifyTransferProof = new BigInteger(Hex.decode("01000002"));
    Set<String> verifyTransferProofSet = new HashSet<>();
    BigInteger verifyBurnProof = new BigInteger(Hex.decode("01000003"));
    Set<String> verifyBurnProofSet = new HashSet<>();
    BigInteger pedersenHash = new BigInteger(Hex.decode("01000004"));
    Set<String> pedersenHashSet = new HashSet<>();
    Set<String> haveAllSet = new HashSet<>();
    LevelDbDataSourceImpl codeStore = null;
    codeStore
        .iterator()
        .forEachRemaining(
            e -> {
              byte[] code = e.getValue();
              if (code != null) {
                String address = StringUtil.encode58Check(e.getKey());
                compile(code, null)
                    .forEach(
                        op -> {
                          if (op.isPush4Op()) {
                            if (op.opd.equals(verifyMintProof)
                                && !haveAllSet.contains(address)
                                && !verifyMintProofSet.contains(address)) {
                              verifyMintProofSet.add(address);
                            } else if (op.opd.equals(verifyTransferProof)
                                && !haveAllSet.contains(address)
                                && !verifyTransferProofSet.contains(address)) {
                              verifyTransferProofSet.add(address);
                            } else if (op.opd.equals(verifyBurnProof)
                                && !haveAllSet.contains(address)
                                && !verifyBurnProofSet.contains(address)) {
                              verifyBurnProofSet.add(address);
                            } else if (op.opd.equals(pedersenHash)
                                && !haveAllSet.contains(address)
                                && !pedersenHashSet.contains(address)) {
                              pedersenHashSet.add(address);
                            }

                            if (verifyMintProofSet.contains(address)
                                && verifyTransferProofSet.contains(address)
                                && verifyBurnProofSet.contains(address)
                                && pedersenHashSet.contains(address)) {
                              haveAllSet.add(address);
                              verifyMintProofSet.remove(address);
                              verifyTransferProofSet.remove(address);
                              verifyBurnProofSet.remove(address);
                              pedersenHashSet.remove(address);
                            }
                          }
                        });
              }
            });
    System.out.println("[HaveAll]:" + haveAllSet);
    System.out.println("[VerifyMintProof]:" + verifyMintProofSet);
    System.out.println("[VerifyTransferProof]:" + verifyTransferProofSet);
    System.out.println("[VerifyBurnProof]:" + verifyBurnProofSet);
    System.out.println("[PedersenHash]:" + pedersenHashSet);
  }

  public static void doScanFigurePoints() {
    Set<String> unknownFigurePoints = new HashSet<>();
    Set<String> knownFigurePoints = new HashSet<>();
    Map<String, Integer> figurePoints = new HashMap<>();
    LevelDbDataSourceImpl codeStore = null;
    codeStore
        .iterator()
        .forEachRemaining(
            e -> {
              byte[] code = e.getValue();
              if (code != null) {
                String address = StringUtil.encode58Check(e.getKey());
                compile(
                    code,
                    opRDs -> {
                      int l = opRDs.size();
                      OpRD op = opRDs.get(l - 1);
                      if (op.isCallOp()) {
                        OpRD op3 = opRDs.get(l - 2);
                        OpRD op2 = opRDs.get(l - 3);
                        OpRD op1 = opRDs.get(l - 4);
                        OpRD op0 = opRDs.get(l - 5);
                        //            if (op3.isPushOp() && !constants.contains(op3.opd)) {
                        //              constants.add(op3.opd);
                        //              System.out.println(op3.opd);
                        //            }
                        //            String figurePoint = String.format("%s,%s,%s,%s,%s", op0, op1,
                        // op2, op3, op);
                        if (!op3.isPushOp()
                            && !op3.is(Op.GAS)
                            && !(op3.is(Op.SUB) && op2.is(Op.GAS))
                            && !(op3.is(Op.DUP4)
                                && op2.is(Op.DUP9)
                                && op1.is(Op.DUP6)
                                && op0.is(Op.DUP2))) {
                          StringBuilder sb = new StringBuilder();
                          // generate figure point
                          boolean findGasOp = false;
                          for (int i = 1;
                              i <= 250 && l - i > 0 && opRDs.get(l - i - 1).opr != Op.CALL;
                              i++) {
                            OpRD curOp = opRDs.get(l - i - 1);
                            sb.append(curOp).append(" ");
                            if (curOp.maybeGas()) {
                              findGasOp = true;
                              break;
                            }
                          }
                          if (!findGasOp) {
                            unknownFigurePoints.add(sb.toString());
                            System.out.println(address + " Unknown");
                            return false;
                          } else {
                            knownFigurePoints.add(sb.toString());
                          }
                        }
                      }
                      return true;
                    });
                knownFigurePoints.forEach(
                    fp -> figurePoints.put(fp, figurePoints.getOrDefault(fp, 0) + 1));
              }
            });
    System.out.println("[Unknown]:");
    unknownFigurePoints.forEach(System.out::println);
    System.out.println("[Stats]:");
    Map<String, int[]> stats = new HashMap<>();
    figurePoints.forEach(
        (k, v) -> {
          String[] ops = k.split(" ");
          String gasOp = ops[ops.length - 1];
          if (!stats.containsKey(gasOp)) {
            stats.put(gasOp, new int[500]);
          }
          stats.get(gasOp)[ops.length] += 1;
        });
    stats.forEach((k, v) -> printRecords(v, k));
  }

  private static int recordDistance(int[] counts, String figurePoint) {
    int distance = figurePoint.split(" ").length;
    counts[distance] += 1;
    return distance;
  }

  private static void printRecords(int[] counts, String feature) {
    System.out.println("[" + feature + "]:");
    int total = 0;
    for (int i = 0; i < counts.length; i++) {
      if (counts[i] != 0) {
        total += counts[i];
        System.out.println(" - " + i + ": " + counts[i]);
      }
    }
    System.out.println(" - total: " + total);
  }

  private static class OpRDs {
    private List<OpRD> opRDs = new LinkedList<>();
  }

  private static class OpRD {
    int opr;
    BigInteger opd;

    OpRD(int opr) {
      this.opr = opr;
    }

    public boolean isPushOp() {
      return opr >= Op.PUSH1 && opr <= Op.PUSH32;
    }

    public boolean isPush4Op() {
      return opr == Op.PUSH4;
    }

    public boolean isCallOp() {
      return opr == Op.CALL || opr == Op.CALLCODE || opr == Op.DELEGATECALL || opr == Op.STATICCALL;
    }

    public boolean is(int target) {
      return target == opr;
    }

    public boolean maybeGas() {
      return opr == Op.GAS
          || (isPushOp()
              && (opd.intValue() == 2300
                  || opd.intValue() == 3000
                  || opd.intValue() == 9000
                  || opd.intValue() == 9700
                  || opd.intValue() == 20000
                  || opd.intValue() == 30000
                  || opd.intValue() == 40000
                  || opd.intValue() == 50000
                  || opd.intValue() == 70000));
    }

    @Override
    public String toString() {
      if (opd != null) {
        byte[] data = opd.toByteArray();
        if (data.length != 1 && data[0] == 0) {
          byte[] tmp = new byte[data.length - 1];
          System.arraycopy(data, 1, tmp, 0, tmp.length);
          data = tmp;
        }
        return String.format("0x%02x", opr)
            + "-"
            + (opr < 0x63 ? opd.toString() : "0x" + Hex.toHexString(data));
      }
      return String.format("0x%02x", opr);
    }

    public String toCode() {
      String dataStr = "";
      if (opd != null) {
        byte[] tmp = new byte[opr - Op.PUSH1 + 1];
        byte[] data = opd.toByteArray();
        for (int i = tmp.length - 1, j = data.length - 1; i >= 0 && j >= 0; i--, j--) {
          tmp[i] = data[j];
        }
        dataStr = Hex.toHexString(tmp);
      }
      return Hex.toHexString(new byte[] {(byte) opr}) + dataStr;
    }
  }

  public static List<OpRD> compile(byte[] ops, Predicate<List<OpRD>> predicate) {
    int len = ops.length;
    int end = Integer.MAX_VALUE;
    List<OpRD> opRDs = new LinkedList<>();
    for (int i = 0; i < len && i < end; ++i) {
      // see if match the corba data prefix
      if (i != len - 1) {
        int curByte = ops[i] & 0xFF;
        int nextByte = ops[i + 1] & 0xFF;
        if ((curByte == 0xa1 && (nextByte == 0x64 || nextByte == 0x65))
            || (curByte == 0xa2 && (nextByte == 0x64 || nextByte == 0x65))
            || (curByte == 0xa3 && (nextByte == 0x64 || nextByte == 0x65))) {
          int cborIndex = doScanCbor(i, ops);
          if (cborIndex != -1) {
            i = cborIndex;
            if ((len - i - 1) % 32 == 0) {
              break;
            }
            continue;
          }
        }
      }

      Integer op = ops[i] & 0xFF;

      if (op == null) {
        break;
      }

      if (op.equals(Op.CODECOPY)) {
        int size = opRDs.size();
        OpRD op3 = opRDs.get(size - 1);
        OpRD op2 = opRDs.get(size - 2);
        OpRD op1 = opRDs.get(size - 3);
        OpRD op0 = opRDs.get(size - 4);
        // contract code or constant push data
        int data = Integer.MAX_VALUE;
        if (op3.is(Op.DUP4) && op2.isPushOp()) {
          data = op2.opd.intValueExact();
        }
        // constant string data
        if (op3.is(Op.SWAP2) && op1.isPushOp()) {
          data = op1.opd.intValueExact();
        }
        // yul datacopy buildin
        if (op3.is(Op.ADD) && op2.is(Op.DUP4) && op0.isPushOp()) {
          data = op0.opd.intValueExact();
        }
        end = Math.min(end, data);
      }

      OpRD opRD = new OpRD(op);
      if (opRD.isPushOp()) {
        int pushDataLen = op - Op.PUSH1 + 1;
        if (i + pushDataLen + 1 < len) {
          byte[] data = new byte[pushDataLen];
          System.arraycopy(ops, i + 1, data, 0, pushDataLen);
          opRD.opd = new BigInteger(1, data);
        }
        i += pushDataLen;
      }
      opRDs.add(opRD);
      if (predicate != null && !predicate.test(opRDs)) {
        break;
      }
    }
    return opRDs;
  }

  private static int doScanCbor(int start, byte[] ops) {
    try {
      int i = start, mapLen = (ops[i++] & 0xFF) - 0xa0;
      for (int j = 0; j < mapLen * 2; j++) {
        int keyType = ops[i++] & 0xFF;
        if (keyType < 0x58) { // bytes(0) ~ bytes(23)
          i += keyType - 0x40;
        } else if (keyType < 0x60) { // bytes(24) ~ upper
          int keyLen = keyType - 0x57;
          int dataLen = readBytes(ops, i, keyLen);
          i += keyLen + dataLen;
        } else if (keyType < 0x78) { // text(0) ~ text(23)
          i += keyType - 0x60;
        } else if (keyType < 0x80) {
          int keyLen = keyType - 0x77;
          int dataLen = readBytes(ops, i, keyLen);
          i += keyLen + dataLen;
        } else if (keyType != 0xF4 && keyType != 0xF5) {
          return -1;
        }
      }
      if (i < ops.length - 1 && i - start == readBytes(ops, i, 2)) {
        return i + 1;
      }
    } catch (ArrayIndexOutOfBoundsException ignored) {
    }
    return -1;
  }

  private static int readBytes(byte[] data, int start, int length) {
    byte[] lenBytes = new byte[length];
    System.arraycopy(data, start, lenBytes, 0, length);
    return new BigInteger(lenBytes).intValue();
  }

  private static void buildStorageStore() {
    //    int sections = Parameters.getInstance().getSections();
    //    int items = Parameters.getInstance().getItems();
    //    System.out.println("Storage store build start.");
    //    System.out.printf("Total sections: %d, items in one section: %d%n", sections, items);
    //    System.out.printf("Total items will be %d%n", sections * items * 2);
    //
    //    LevelDbDataSourceImpl rowStore = DbUtils.openDb("storage-row");
    //    long start = System.currentTimeMillis();
    //
    //    for (int i = 1; i <= sections; i++) {
    //      if (i % 100 == 0) {
    //        System.out.printf("section\t%d~%d done, cost: %ds%n",
    //            i - 99, i, (System.currentTimeMillis() - start) / 1000);
    //        start = System.currentTimeMillis();
    //      }
    //
    //      byte[] prefix = Sha256Hash.hash(BigInteger.valueOf(i).toByteArray());
    //      for (int j = 1; j <= items; j++) {
    //        int idx = i * items + j;
    //        byte[] idxBytes = BigInteger.valueOf(idx).toByteArray();
    //        byte[] randKey = Sha256Hash.hash(idxBytes);
    //        byte[] prefixKey = combine(prefix, randKey);
    //        byte[] value = Sha256Hash.hashTwice(randKey);
    //
    //        rowStore.putData(randKey, value);
    //        rowStore.putData(prefixKey, value);
    //      }
    //    }
    //    rowStore.closeDB();
    //    System.out.println("Storage store build completed.");
  }

  private static byte[] combine(byte[] a, byte[] b) {
    byte[] ret = a.clone();
    System.arraycopy(b, 16, ret, 16, 16);
    return ret;
  }

  private static void printStressParameters() {
    //    String mode;
    //    if (Parameters.getInstance().getMode() == 0) mode = "only read";
    //    else if (Parameters.getInstance().getMode() == 1) mode = "only write";
    //    else mode = "read then write";
    //    System.out.println("Current test mode: " + mode);
    //    System.out.printf("Current max open file: %d%n",
    // Parameters.getInstance().getMaxOpenFile());
    //    System.out.printf("Total loop: %d, times in one loop: %d%n",
    //        Parameters.getInstance().getLoops(), Parameters.getInstance().getTimes());
  }

  private static void doRandKeyStorageStressTest(int mode) {
    //    int loops = Parameters.getInstance().getLoops();
    //    int times = Parameters.getInstance().getTimes();
    //
    //    System.out.println("Rand key stress test start.");
    //    printStressParameters();
    //
    //    LevelDbDataSourceImpl rowStore = DbUtils.openDb("storage-row");
    //    Random random = new Random();
    //    long totalCost = 0;
    //    for (int i = 1; i <= loops; i++) {
    //      long start = System.nanoTime();
    //      byte[] value = new byte[32];
    //      for (int j = 0; j < times; j++) {
    //        int section = random.nextInt(Parameters.getInstance().getSections()) + 1;
    //        int index = random.nextInt(Parameters.getInstance().getItems()) + 1;
    //        value = doActionByMode(rowStore, getRandKey(section, index), mode);
    //      }
    //      totalCost += System.nanoTime() - start;
    //      System.out.printf("loop: %d, avg: %.2fus, total avg: %.2fus, final value: %s%n",
    //          i,
    //          (double)(System.nanoTime() - start) / times / 1000,
    //          (double)totalCost / i / times / 1000,
    //          Hex.toHexString(value));
    //    }
    //    rowStore.closeDB();
  }

  private static void doPrefixKeyStorageStressTest(int mode) {
    //    int loops = Parameters.getInstance().getLoops();
    //    int times = Parameters.getInstance().getTimes();
    //
    //    System.out.println("Prefix key stress test start.");
    //    printStressParameters();
    //
    //    LevelDbDataSourceImpl rowStore = DbUtils.openDb("storage-row");
    //    Random random = new Random();
    //    long totalCost = 0;
    //    for (int i = 1; i <= loops; i++) {
    //      long start = System.nanoTime();
    //      byte[] value = new byte[32];
    //      int section = random.nextInt(Parameters.getInstance().getSections()) + 1;
    //      for (int j = 0; j < times; j++) {
    //        int index = random.nextInt(Parameters.getInstance().getItems()) + 1;
    //        value = doActionByMode(rowStore, getPrefixKey(section, index), mode);
    //      }
    //      totalCost += System.nanoTime() - start;
    //      System.out.printf("loop: %d, avg: %.2fus, total avg: %.2fus, final value: %s%n",
    //          i,
    //          (double)(System.nanoTime() - start) / times / 1000,
    //          (double)totalCost / i / times / 1000,
    //          Hex.toHexString(value));
    //    }
    //    rowStore.closeDB();
  }

  private static byte[] doActionByMode(LevelDbDataSourceImpl store, byte[] key, int mode) {
    if (mode == 0) {
      return store.getData(key);
    } else if (mode == 1) {
      byte[] data = generateData();
      store.putData(key, data);
      return data;
    } else {
      byte[] data = store.getData(key);
      Random random = new Random();
      data[random.nextInt(32)] = (byte) random.nextInt();
      store.putData(key, data);
      return data;
    }
  }

  private static byte[] getRandKey(int section, int index) {
    return Sha256Hash.hash(true, BigInteger.valueOf(section * 10_000L + index).toByteArray());
  }

  private static byte[] getPrefixKey(int section, int index) {
    byte[] randKey = getRandKey(section, index);
    return combine(Sha256Hash.hash(true, BigInteger.valueOf(section).toByteArray()), randKey);
  }

  private static byte[] generateData() {
    Random random = new Random();
    byte[] data = new byte[32];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) random.nextInt();
    }
    return data;
  }

  private static int findFirstArg(String[] args) {
    for (int i = 0; i < args.length; i++) {
      if (args[i].contains("--") || args[i].contains("-")) return i;
    }
    return args.length;
  }

  private static void doTestBloomFilter() {
    //    int total = 100_000_000;
    //    byte[][] origin = new byte[total][];
    //    BitSet bloom = new BitSet();
    //    Random rand = new Random(System.nanoTime());
    //    for (int i = 0; i < total; i++) {
    //      byte[] address = new byte[20];
    //      rand.nextBytes(address);
    //      if (i % 10 == 0) {
    //        // put it into bloom
    //        byte[] hash = Hash.keccak256(address);
    //        int index = new BigInteger(Hex.toHexString(hash).substring(2, 10)).intValue();
    //        bloom.set(index);
    //      }
    //      if (i % 100_000 == 0) {
    //        // report process
    //        System.out.println("Processed " + i);
    //      }
    //    }
  }

  public static void main(String[] args) {
    int index = findFirstArg(args);
    String[] subCmd = Arrays.copyOfRange(args, 0, index);
    args = index == args.length ? new String[] {} : Arrays.copyOfRange(args, index, args.length);
    //    JCommander.newBuilder().addObject(Parameters.getInstance()).build().parse(args);
    //
    //    if (subCmd.length < 2) {
    //      System.out.println("This program needs at least two arg.");
    //    } else {
    //      Cli cli = new Cli();
    //      cli.serve(subCmd[0], subCmd[1], Arrays.copyOfRange(subCmd, 2, subCmd.length));
    //    }
    //    doScanShieldMethods();
    //    doTestBloomFilter();
    //    doScanABI();
    //    doRepair();
    //    doScanChainID();
    //    doScanFigurePoint();
    //    doScanSpecialOpCode();
    //    doScanFigurePoints();
    //
    // doRemoveFigurePoint(Hex.decode("608060405234801561001057600080fd5b50d3801561001d57600080fd5b50d2801561002a57600080fd5b50600436106102f05760003560e01c80637eba46a91161019c578063cb7ccd7211610103578063e7fb4454116100bc578063ef299b0b11610096578063ef299b0b146106e9578063efa6a7f514610706578063f0ef85dd1461070e578063f77c479114610716576102f0565b8063e7fb445414610692578063e8f23c25146106af578063ebff8707146106cc576102f0565b8063cb7ccd7214610662578063d679098a1461066a578063d7db3d4514610672578063d8d40cee1461067a578063db81903e14610682578063de7d4fc41461068a576102f0565b8063ab86ba3a11610155578063ab86ba3a1461061d578063ac6f990014610625578063b1148eaf1461062d578063b2930e051461064a578063be9a655514610652578063c7bf4b4f1461065a576102f0565b80637eba46a9146105375780638bdff161146105545780638c1ddded1461055c578063955d14cd146105795780639af14389146105815780639ccc0e84146105a7576102f0565b8063432050601161025b5780636a72be40116102145780636e81f1d1116101ee5780636e81f1d1146105025780636f64f8811461050a57806370b7e4021461051257806375b4d78c1461052f576102f0565b80636a72be401461049b5780636b84a3e9146104c15780636e7ba9db146104fa576102f0565b806343205060146104165780634d6b85511461043c57806358b2b7441461044457806361de922e1461044c57806367e8d3d2146104725780636854171d1461047a576102f0565b80631c8538b9116102ad5780631c8538b91461039557806320c9ad2c146103b257806323a58292146103ba57806330818189146103c257806330dd309e146103ca5780633cebb823146103f0576102f0565b80630121b93f146102f557806307da68f5146103265780630c606ce7146103305780630d4f27311461034a57806313a2f4261461035257806317e190fa1461036f575b600080fd5b6103126004803603602081101561030b57600080fd5b503561071e565b604080519115158252519081900360200190f35b61032e6108b4565b005b6103386108dc565b60408051918252519081900360200190f35b6103386108e2565b6103126004803603602081101561036857600080fd5b50356108e8565b6103386004803603602081101561038557600080fd5b50356001600160a01b0316610a30565b610312600480360360208110156103ab57600080fd5b5035610a4b565b610338610aab565b610338610ab1565b610338610ab7565b610338600480360360208110156103e057600080fd5b50356001600160a01b0316610abd565b61032e6004803603602081101561040657600080fd5b50356001600160a01b0316610ad8565b6103386004803603602081101561042c57600080fd5b50356001600160a01b0316610b11565b610338610b2c565b610338610b32565b6103386004803603602081101561046257600080fd5b50356001600160a01b0316610b38565b610338610b53565b610482610b59565b6040805192835260208301919091528051918290030190f35b610338600480360360208110156104b157600080fd5b50356001600160a01b0316610b62565b6104de600480360360208110156104d757600080fd5b5035610b7d565b604080516001600160a01b039092168252519081900360200190f35b610338610bba565b610338610bc0565b6104de610bc6565b6103386004803603602081101561052857600080fd5b5035610bd5565b610312610be7565b6103126004803603602081101561054d57600080fd5b5035610c2b565b610338610c4c565b6103126004803603602081101561057257600080fd5b5035610c52565b610338610c73565b6103386004803603602081101561059757600080fd5b50356001600160a01b0316610c79565b6105cd600480360360208110156105bd57600080fd5b50356001600160a01b0316610c94565b60408051602080825283518183015283519192839290830191858101910280838360005b838110156106095781810151838201526020016105f1565b505050509050019250505060405180910390f35b610338610ca7565b610338610cad565b6103126004803603602081101561064357600080fd5b5035610cb3565b610338610cd4565b61032e610cda565b610338610d04565b6104de610d0a565b610338610d19565b6104de610d1f565b610338610d2e565b610312610d34565b610338610d42565b610338600480360360208110156106a857600080fd5b5035610d48565b610312600480360360208110156106c557600080fd5b5035610d5a565b610338600480360360208110156106e257600080fd5b5035610ea1565b61032e600480360360208110156106ff57600080fd5b5035610eb3565b6104de610ff0565b610312610fff565b6104de611008565b601e5460009060ff16801561073b5750601e54610100900460ff16155b61074457600080fd5b60045433600090815260186020526040902054908302111561076557600080fd5b6000821161077257600080fd5b60075482111561078157600080fd5b60135433600090815260176020526040902054839190600114156107aa576000925050506108af565b3360008181526017602090815260408083206001908190556010805485526014909352922080546001600160a01b03191690931790925581540190556007548211610868573360009081526018602052604090205460045461080e91908602611017565b33600081815260186020908152604080832094909455601181528382208890556013805460158352858420556016825284832089905580548901908190558252601290529190912080546001600160a01b03191690911790555b604080513381526020810186905280820183905290517f9ed6df84f8e831daadba9609591ae5a511e7f19758b47dfa991bd18af44903d39181900360600190a16001925050505b919050565b6000546001600160a01b031633146108cb57600080fd5b601e805461ff001916610100179055565b60055481565b600c5481565b3360009081526018602052604081205482111561090457600080fd5b60015460408051336024820152604480820186905282518083039091018152606490910182526020810180516001600160e01b0316600160e01b63a9059cbb02178152915181516000946060946001600160a01b0390911693620f4240939092909182918083835b6020831061098b5780518252601f19909201916020918201910161096c565b6001836020036101000a03801982511681845116808217855250505050505090500191505060006040518083038160008787f1925050503d80600081146109ee576040519150601f19603f3d011682016040523d82523d6000602084013e6109f3565b606091505b50915091508115610a295733600090815260186020526040902054610a189085611017565b336000908152601860205260409020555b5092915050565b6001600160a01b031660009081526016602052604090205490565b601e5460009060ff168015610a685750601e54610100900460ff16155b610a7157600080fd5b8115610a7c57600080fd5b33600090815260176020526040902054610a9557600080fd5b5033600090815260116020526040902055600190565b60105490565b600f5481565b60095481565b6001600160a01b031660009081526017602052604090205490565b6000546001600160a01b03163314610aef57600080fd5b600080546001600160a01b0319166001600160a01b0392909216919091179055565b6001600160a01b031660009081526015602052604090205490565b600b5490565b60135490565b6001600160a01b031660009081526011602052604090205490565b600a5481565b60105460135482565b6001600160a01b031660009081526018602052604090205490565b6000818152601a60205260408120546001600160a01b0316610b9e57600080fd5b506000908152601a60205260409020546001600160a01b031690565b600a5490565b60045481565b6002546001600160a01b031681565b6000908152601b602052604090205490565b6000600d54600f54420311610bfb57600080fd5b601354600a1115610c0b57600080fd5b610c1361102e565b6000601355610c206114fa565b505042600f55600190565b600080546001600160a01b03163314610c4357600080fd5b50600555600190565b60095490565b600080546001600160a01b03163314610c6a57600080fd5b50600655600190565b600f5490565b6001600160a01b031660009081526019602052604090205490565b5060408051600081526020810190915290565b600b5481565b60055490565b600080546001600160a01b03163314610ccb57600080fd5b50600755600190565b600d5481565b6000546001600160a01b03163314610cf157600080fd5b601e805460ff1916600117905542600f55565b60065481565b6003546001600160a01b031681565b60075481565b600e546001600160a01b031681565b60085481565b601e54610100900460ff1681565b60085490565b6000908152601d602052604090205490565b33600090815260196020526040812054821115610d7657600080fd5b60035460408051336024820152604480820186905282518083039091018152606490910182526020810180516001600160e01b0316600160e01b63a9059cbb02178152915181516000946060946001600160a01b0390911693620f4240939092909182918083835b60208310610dfd5780518252601f199092019160209182019101610dde565b6001836020036101000a03801982511681845116808217855250505050505090500191505060006040518083038160008787f1925050503d8060008114610e60576040519150601f19603f3d011682016040523d82523d6000602084013e610e65565b606091505b50915091508115610a295733600090815260196020526040902054610e8a9085611017565b336000908152601960205260409020555092915050565b6000908152601c602052604090205490565b60008111610ec057600080fd5b60015460408051336024820152306044820152606480820185905282518083039091018152608490910182526020810180516001600160e01b0316600160e01b6323b872dd02178152915181516000946060946001600160a01b0390911693620f4240939092909182918083835b60208310610f4d5780518252601f199092019160209182019101610f2e565b6001836020036101000a03801982511681845116808217855250505050505090500191505060006040518083038160008787f1925050503d8060008114610fb0576040519150601f19603f3d011682016040523d82523d6000602084013e610fb5565b606091505b50915091508115610feb5733600090815260186020526040902054610fda90846117bf565b336000908152601860205260409020555b505050565b6001546001600160a01b031681565b601e5460ff1681565b6000546001600160a01b031681565b60008282111561102357fe5b508082035b92915050565b601e5460ff1680156110485750601e54610100900460ff16155b61105157600080fd5b42600f5560135460001943014090600061106a836117d5565b9050600060028206600114611080576000611083565b60015b6004546013546009546005549394509102916064601e8402819004928301913360009081526019602052604081208054939092049092019055601354600116156110e857846110db57601354600290046001016110e3565b601354600290045b6110f0565b601354600290045b9050600060026010600301548161110357fe5b061561112a578561111a5760135460029004611125565b601354600290046001015b611132565b601354600290045b90506000826064600554601e028161114657fe5b048161114e57fe5b04905060008260646005546045028161116357fe5b048161116b57fe5b6008546000908152601b602052604081208e905591900491508080805b6013548110156112b157808d14156111fc57600084815260126020818152604080842054600880548652601a845282862080546001600160a01b0319166001600160a01b03938416179055548552601c83528185208e905585855292825280842054909216835260189052902080548a0190555b8b1561122d57600281066001141561122357600454998a0199929092019190850190611228565b908401905b61124f565b6002810661124a57600454998a019992909201919085019061124f565b908401905b6000818152601260205260409020546001600160a01b0316156112a957600093845260126020908152604080862080546001600160a01b039081168852601884528288209690965554909416855260199052918320559080825b600101611188565b5081601860006010600201600087815260200190815260200160002060009054906101000a90046001600160a01b03166001600160a01b03166001600160a01b031681526020019081526020016000208190555080601960006010600201600087815260200190815260200160002060009054906101000a90046001600160a01b03166001600160a01b03166001600160a01b03168152602001908152602001600020819055507f4afdb1a73d13beb867a0b5546b180b0091e2aa3c88f21390db35105a43a1c8f2338f8a60405180846001600160a01b03166001600160a01b03168152602001838152602001828152602001935050505060405180910390a1600a8054606460058d028190049091018255908b0281900490600f8c02048a018b11156113ea57600b8054606460558e0204018b900390555b6013548d1061140257600980546064601e8e02040190555b6008546000908152601d602052604090208d905560055461142490829061197c565b5060088054600101908190556175301061145b576127106008548161144557fe5b0661145b5760026005548161145657fe5b046005555b60006008541180156114845750600d5462049d408161147657fe5b046008548161148157fe5b06155b156114a257611497600a5460055461197c565b156114a2576000600a555b60006008541180156114cb5750600d5462093a80816114bd57fe5b04600854816114c857fe5b06155b156114e9576114de600b5460055461197c565b156114e9576000600b555b505050505050505050505050505050565b6000606060106000015460405190808252806020026020018201604052801561152d578160200160208202803883390190505b5090506000805b601054811015611728576000818152601460209081526040808320546001600160a01b031683526011909152902054611591576000818152601460209081526040808320546001600160a01b031683526017909152812055611720565b6004546000828152601460209081526040808320546001600160a01b0316835260118252808320546018909252909120549102116116e8576000818152601460209081526040808320546001600160a01b0316835260188252808320546004546011909352922054611604929102611017565b600082815260146020818152604080842080546001600160a01b039081168652601884529185209590955592859052529054845160018501949190921691859190811061164d57fe5b6001600160a01b0392831660209182029290920181019190915260008381526014825260408082208054851683526017845281832060019055601380548254871685526015865283852081905582549085526012865283852080546001600160a01b03191691881691909117905581548616845260118086528385205460168752848620559154909516835290925220548154019055611720565b600081815260146020908152604080832080546001600160a01b03908116855260118452828520859055905416835260179091528120555b600101611534565b5060005b60105481101561175a57600081815260146020526040902080546001600160a01b031916905560010161172c565b50600060108190555b818110156117b75782818151811061177757fe5b60209081029190910181015160008381526014909252604090912080546001600160a01b0319166001600160a01b03909216919091179055600101611763565b506010555090565b6000828201838110156117ce57fe5b9392505050565b601354600090816201000082111561188357601084601d1a60f81b60f81c60ff16816117fd57fe5b0660ff166201000002601085601e6020811061181557fe5b1a60f81b60f81c60ff168161182657fe5b0460ff1661100002601086601e6020811061183d57fe5b1a60f81b60f81c60ff168161184e57fe5b0660ff166101000286601f6020811061186357fe5b1a60f81b60f81c60010260ff16010161ffff160162ffffff1690506117ce565b6110008211156118fa57601084601e1a60f81b60f81c60ff16816118a357fe5b0460ff1661100002601085601e602081106118ba57fe5b1a60f81b60f81c60ff16816118cb57fe5b0660ff166101000285601f602081106118e057fe5b1a60f81b60f81c60010260ff16010161ffff1690506117ce565b61010082111561194857601084601e1a60f81b60f81c60ff168161191a57fe5b0660ff166101000284601f6020811061192f57fe5b1a60f81b60f81c60010260ff160161ffff1690506117ce565b6010821115611962575060ff831660f890811b901c6117ce565b601060ff851660f890811b901c0660ff1690509392505050565b6002546003546040805160248101869052604481018590526001606482015263337f9800420160848201523060a48201526001600160a01b0392831660c4808301919091528251808303909101815260e490910182526020810180516001600160e01b0316600160e01b63f552d91b02178152915181516000958695606095911693620f4240939092909182918083835b60208310611a2c5780518252601f199092019160209182019101611a0d565b6001836020036101000a03801982511681845116808217855250505050505090500191505060006040518083038160008787f1925050503d8060008114611a8f576040519150601f19603f3d011682016040523d82523d6000602084013e611a94565b606091505b50915091508115611aaa57600192505050611028565b5050600a80548401905550600061102856fea165627a7a72305820d02b505a146d79ca191fcf62730a59e496732d95eb8a387afc2587d8859248680029"));
    //    doScanContract();
    //    doScanAccount();
    //    doScanMultiValidate();
    //
    // System.out.println(compile(Hex.decode("50fea26474726f6e5820cb75fb79a1df845fbfba8f92be1c7ea5e8219dd8ad7a397043e26d773fd67fcc64736f6c634300050c0031")));
    //    BigInteger v = new
    // BigInteger(Hex.decode("3d7f10a114070cb8e3a8a5c4be9aa232832d43a456cca7db"));
    //    BigInteger id = new
    // BigInteger(Hex.decode("1ebf88508a03865c71d452e25f4d51194196a1d22b6653dc"));
    //    System.out.println(v.subtract(id.multiply(BigInteger.valueOf(2))));
    //    System.out.println(doScanCbor(0,
    // Hex.decode("a26474726f6e582212205b9b5f6c16fef557410789a433209704a2d8a24af70e59c62a1c0b50eb71736364736f6c63430008060033")));
    //    doScanAccount();
    //
    // System.out.println(compile(Hex.decode(("a265627a7a7230582096871d01e767251f6b1b8dcf71b0c918bc49eef992de18a827d1220652469b5f6c6578706572696d656e74616cf50037bc49eef992de18a827d1220652469b5f6c6578706572696d656e74616cf50037")), ""));
  }
}
