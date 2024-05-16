package org.tron.core.store;

import java.util.Objects;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.tron.common.utils.ByteUtil;
import org.tron.core.capsule.ContractStateCapsule;
import org.tron.core.db.TronStoreWithRevoking;

@Slf4j(topic = "DB")
@Component
public class ContractStateStore extends TronStoreWithRevoking<ContractStateCapsule> {

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

  public ContractStateCapsule getAccountRecord(byte[] addr) {
    addr[0] = (byte) 0x42;
    return getUnchecked(getCurrentPrefixKey(addr));
  }

  public ContractStateCapsule getAccountRecord(long cycleNumber, byte[] addr) {
    addr[0] = (byte) 0x42;
    return getUnchecked(addPrefix(cycleNumber, addr));
  }

  public void setAccountRecord(byte[] addr, ContractStateCapsule item) {
    addr[0] = (byte) 0x42;
    revokingDB.put(getCurrentPrefixKey(addr), item.getData());
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
}
