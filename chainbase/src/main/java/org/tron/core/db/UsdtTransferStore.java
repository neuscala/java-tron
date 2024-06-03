package org.tron.core.db;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.tron.common.es.ExecutorServiceManager;
import org.tron.common.utils.ByteUtil;
import org.tron.core.capsule.UsdtTransferCapsule;
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.core.store.DynamicPropertiesStore;

@Slf4j(topic = "DB")
@Component
public class UsdtTransferStore extends TronStoreWithRevoking<UsdtTransferCapsule> {

  private ExecutorService queryThreadPool = ExecutorServiceManager.newFixedThreadPool("UsdtTransferStore-query", 16);

  @Autowired
  private DynamicPropertiesStore dps;

  @Autowired
  private UsdtTransferStore(@Value("usdt-transfer") String dbName) {
    super(dbName);
  }

  @Override
  public UsdtTransferCapsule get(byte[] key) {
    return getUnchecked(key);
  }

  @Override
  public void put(byte[] key, UsdtTransferCapsule item) {
    if (Objects.isNull(key) || Objects.isNull(item)) {
      return;
    }

    revokingDB.put(key, item.getData());
  }

  public UsdtTransferCapsule getRecord(byte[] txID) {
    return getUnchecked(getCurrentPrefixKey(txID));
  }

  public UsdtTransferCapsule getRecord(long cycleNumber, byte[] txID) {
    return getUnchecked(addPrefix(cycleNumber, txID));
  }

  public void setRecord(byte[] txID, UsdtTransferCapsule item) {
    revokingDB.put(getCurrentPrefixKey(txID), item.getData());
  }

  private byte[] addPrefix(long cycleNumber, byte[] key) {
    return ByteUtil.merge((cycleNumber + "-").getBytes(), key);
  }
  private byte[] getCurrentPrefixKey(byte[] key) {
    return addPrefix(dps.getCurrentCycleNumber(), key);
  }

  public Map<WrappedByteArray, UsdtTransferCapsule> getCycleData(long cycleNumber) {
    return this.prefixQuery((cycleNumber + "-").getBytes());
  }

  public Map<ByteString, UsdtTransferCapsule> getMergedDataWithinCycles(
      long cycleNumber, long cycleCount, boolean isContract) {
    Map<ByteString, UsdtTransferCapsule> result = new HashMap<>();
    CountDownLatch cdl = new CountDownLatch((int) cycleCount);
    ReentrantLock lock = new ReentrantLock();
    for (int i = 0; i < cycleCount; i++) {
      final int index = i;
      queryThreadPool.submit(() -> {
        byte[] cycleBytes = ((cycleNumber + index) + "-").getBytes();
        byte[] key = new byte[cycleBytes.length + 1];
        System.arraycopy(cycleBytes, 0, key, 0, cycleBytes.length);
        key[key.length - 1] = (byte) (isContract ? 0x41 : 0x42);
        Map<WrappedByteArray, UsdtTransferCapsule> data = this.prefixQuery(key);
        lock.lock();
        try {
          data.forEach((k, v) -> {
            ByteString txID = ByteString.copyFrom(Arrays.copyOfRange(k.getBytes(), 5, 26));
            if (result.containsKey(txID)) {
              result.get(txID).merge(v);
            } else {
              result.put(txID, v);
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

  public UsdtTransferCapsule getAllMergedDataWithinCycles(
      long cycleNumber, long cycleCount, boolean isContract) {
    UsdtTransferCapsule result = new UsdtTransferCapsule();
    CountDownLatch cdl = new CountDownLatch((int) cycleCount);
    ReentrantLock lock = new ReentrantLock();
    for (int i = 0; i < cycleCount; i++) {
      final int index = i;
      queryThreadPool.submit(() -> {
        byte[] cycleBytes = ((cycleNumber + index) + "-").getBytes();
        byte[] key = new byte[cycleBytes.length + 1];
        System.arraycopy(cycleBytes, 0, key, 0, cycleBytes.length);
        key[key.length - 1] = (byte) (isContract ? 0x41 : 0x42);
        Map<WrappedByteArray, UsdtTransferCapsule> data = this.prefixQuery(key);
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
