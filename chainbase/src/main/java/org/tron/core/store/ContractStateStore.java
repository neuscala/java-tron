package org.tron.core.store;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;

import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.tron.common.es.ExecutorServiceManager;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.ByteUtil;
import org.tron.core.capsule.ContractStateCapsule;
import org.tron.core.db.TronStoreWithRevoking;
import org.tron.core.db2.common.WrappedByteArray;

@Slf4j(topic = "DB")
@Component
public class ContractStateStore extends TronStoreWithRevoking<ContractStateCapsule> {

  private ExecutorService queryThreadPool = ExecutorServiceManager.newFixedThreadPool("ContractStateStore-query", 16);

  @Autowired
  private DynamicPropertiesStore dps;

  @Autowired
  private ContractStore cs;

  @Autowired
  private ContractStateStore(@Value("contract-state") String dbName) {
    super(dbName);
  }

  @Override
  public ContractStateCapsule get(byte[] key) {
    return getUnchecked(key);
  }

  @Override
  public void put(byte[] key, ContractStateCapsule item) {
    if (Objects.isNull(key) || Objects.isNull(item)) {
      return;
    }

    revokingDB.put(key, item.getData());
  }

  public ContractStateCapsule getUsdtRecord(long cycleNumber) {
    return getUnchecked(addPrefix(cycleNumber, "usdt".getBytes()));
  }

  public ContractStateCapsule getUsdtRecord() {
    return getUnchecked(getCurrentPrefixKey("usdt".getBytes()));
  }

  public void setUsdtRecord(ContractStateCapsule item) {
    revokingDB.put(getCurrentPrefixKey("usdt".getBytes()), item.getData());
  }

  public ContractStateCapsule getBinanceRecord(long cycleNumber) {
    return getUnchecked(addPrefix(cycleNumber, "Binance".getBytes()));
  }

  public ContractStateCapsule getBinanceRecord() {
    return getUnchecked(getCurrentPrefixKey("Binance".getBytes()));
  }

  public void setBinanceRecord(ContractStateCapsule item) {
    revokingDB.put(getCurrentPrefixKey("Binance".getBytes()), item.getData());
  }

  public ContractStateCapsule getOkexRecord(long cycleNumber) {
    return getUnchecked(addPrefix(cycleNumber, "Okex".getBytes()));
  }

  public ContractStateCapsule getOkexRecord() {
    return getUnchecked(getCurrentPrefixKey("Okex".getBytes()));
  }

  public void setOkexRecord(ContractStateCapsule item) {
    revokingDB.put(getCurrentPrefixKey("Okex".getBytes()), item.getData());
  }

  public ContractStateCapsule getBybitRecord(long cycleNumber) {
    return getUnchecked(addPrefix(cycleNumber, "Bybit".getBytes()));
  }

  public ContractStateCapsule getBybitRecord() {
    return getUnchecked(getCurrentPrefixKey("Bybit".getBytes()));
  }

  public void setBybitRecord(ContractStateCapsule item) {
    revokingDB.put(getCurrentPrefixKey("Bybit".getBytes()), item.getData());
  }

  public ContractStateCapsule getMevTPsRecord(long cycleNumber) {
    return getUnchecked(addPrefix(cycleNumber, "TPsUGKAoXDSFz332ZYtTGdDHWzftLYWFj7".getBytes()));
  }

  public ContractStateCapsule getMevTPsRecord() {
    return getUnchecked(getCurrentPrefixKey("TPsUGKAoXDSFz332ZYtTGdDHWzftLYWFj7".getBytes()));
  }

  public void setMevTPsRecord(ContractStateCapsule item) {
    revokingDB.put(getCurrentPrefixKey("TPsUGKAoXDSFz332ZYtTGdDHWzftLYWFj7".getBytes()), item.getData());
  }

  public ContractStateCapsule getAccountRecord(byte[] addr) {
    byte[] addrKey = addr.clone();
    addrKey[0] = (byte) 0x42;
    return getUnchecked(getCurrentPrefixKey(addrKey));
  }

  public ContractStateCapsule getAccountRecord(long cycleNumber, byte[] addr) {
    byte[] addrKey = addr.clone();
    addrKey[0] = (byte) 0x42;
    return getUnchecked(addPrefix(cycleNumber, addrKey));
  }

  public void setAccountRecord(byte[] addr, ContractStateCapsule item) {
    byte[] addrKey = addr.clone();
    addrKey[0] = (byte) 0x42;
    revokingDB.put(getCurrentPrefixKey(addrKey), item.getData());
  }

  public ContractStateCapsule getContractRecord(byte[] addr) {
    return getUnchecked(getCurrentPrefixKey(addr));
  }

  public ContractStateCapsule getContractRecord(long cycleNumber, byte[] addr) {
    return getUnchecked(addPrefix(cycleNumber, addr));
  }

  public void setContractRecord(byte[] addr, ContractStateCapsule item) {
    revokingDB.put(getCurrentPrefixKey(addr), item.getData());
  }

  private byte[] addPrefix(long cycleNumber, byte[] key) {
    return ByteUtil.merge((cycleNumber + "-").getBytes(), key);
  }
  private byte[] getCurrentPrefixKey(byte[] key) {
    return addPrefix(dps.getCurrentCycleNumber(), key);
  }

  public Map<WrappedByteArray, ContractStateCapsule> getCycleData(long cycleNumber) {
    return this.prefixQuery((cycleNumber + "-").getBytes());
  }

  public Map<ByteString, ContractStateCapsule> getMergedDataWithinCycles(
      long cycleNumber, long cycleCount, boolean isContract) {
    Map<ByteString, ContractStateCapsule> result = new HashMap<>();
    CountDownLatch cdl = new CountDownLatch((int) cycleCount);
    ReentrantLock lock = new ReentrantLock();
    for (int i = 0; i < cycleCount; i++) {
      final int index = i;
      queryThreadPool.submit(() -> {
        byte[] cycleBytes = ((cycleNumber + index) + "-").getBytes();
        byte[] key = new byte[cycleBytes.length + 1];
        System.arraycopy(cycleBytes, 0, key, 0, cycleBytes.length);
        key[key.length - 1] = (byte) (isContract ? 0x41 : 0x42);
        Map<WrappedByteArray, ContractStateCapsule> data = this.prefixQuery(key);
        lock.lock();
        try {
          data.forEach((k, v) -> {
            ByteString addr = ByteString.copyFrom(Arrays.copyOfRange(k.getBytes(), 5, 26));
            if (result.containsKey(addr)) {
              result.get(addr).merge(v);
            } else {
              result.put(addr, v);
            }
          });
        } finally {
          lock.unlock();
        }
        cdl.countDown();
      });
    }
    try {
      cdl.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  public ContractStateCapsule getAllMergedDataWithinCycles(
      long cycleNumber, long cycleCount, boolean isContract) {
    ContractStateCapsule result = new ContractStateCapsule(cycleNumber + cycleCount -1);
    CountDownLatch cdl = new CountDownLatch((int) cycleCount);
    ReentrantLock lock = new ReentrantLock();
    for (int i = 0; i < cycleCount; i++) {
      final int index = i;
      queryThreadPool.submit(() -> {
        byte[] cycleBytes = ((cycleNumber + index) + "-").getBytes();
        byte[] key = new byte[cycleBytes.length + 1];
        System.arraycopy(cycleBytes, 0, key, 0, cycleBytes.length);
        key[key.length - 1] = (byte) (isContract ? 0x41 : 0x42);
        Map<WrappedByteArray, ContractStateCapsule> data = this.prefixQuery(key);
        lock.lock();
        try {
          data.forEach((k, v) -> result.merge(v));
        } finally {
          lock.unlock();
        }
        cdl.countDown();
      });
    }
    try {
      cdl.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return result;
  }
}
