package org.tron.core.store;

import java.text.SimpleDateFormat;
import java.util.Objects;
import java.util.TimeZone;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.tron.common.utils.ByteUtil;
import org.tron.core.capsule.ContractStateCapsule;
import org.tron.core.db.TronStoreWithRevoking;

import javax.annotation.PostConstruct;

import static org.tron.core.config.Parameter.ChainConstant.BLOCK_PRODUCED_INTERVAL;

@Slf4j(topic = "DB")
@Component
public class ContractStateStore extends TronStoreWithRevoking<ContractStateCapsule> {

  public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");

  @Autowired
  private DynamicPropertiesStore dps;

  @Autowired
  private ContractStore cs;

  @Autowired
  private ContractStateStore(@Value("contract-state") String dbName) {
    super(dbName);
  }

  @PostConstruct
  public void init() {
    DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("GMT"));
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

  public ContractStateCapsule getUsdtRecord(long date) {
    return getUnchecked(addPrefix(date, "usdt".getBytes()));
  }

  public ContractStateCapsule getUsdtRecord() {
    return getUnchecked(getCurrentDatePrefixKey("usdt".getBytes()));
  }

  public void setUsdtRecord(ContractStateCapsule item) {
    revokingDB.put(getCurrentDatePrefixKey("usdt".getBytes()), item.getData());
  }

  public ContractStateCapsule getAccountRecord(byte[] addr) {
    addr[0] = (byte) 0x42;
    return getUnchecked(getCurrentDatePrefixKey(addr));
  }

  public void setAccountRecord(byte[] addr, ContractStateCapsule item) {
    addr[0] = (byte) 0x42;
    revokingDB.put(getCurrentDatePrefixKey(addr), item.getData());
  }

  public ContractStateCapsule getContractRecord(byte[] addr) {
    return getUnchecked(getCurrentDatePrefixKey(addr));
  }

  public void setContractRecord(byte[] addr, ContractStateCapsule item) {
    revokingDB.put(getCurrentDatePrefixKey(addr), item.getData());
  }

  public ContractStateCapsule getTRC10Record(byte[] tokenName) {
    return getUnchecked(getCurrentDatePrefixKey(getTRC10Key(tokenName)));
  }

  public void setTRC10Record(byte[] tokenName, ContractStateCapsule item) {
    revokingDB.put(getCurrentDatePrefixKey(getTRC10Key(tokenName)), item.getData());
  }

  private byte[] getTRC10Key(byte[] tokenName) {
    byte[] key = new byte[tokenName.length + 1];
    key[0] = (byte) 0x51;
    System.arraycopy(tokenName, 0, key, 1, tokenName.length);
    return key;
  }

  private byte[] addPrefix(long date, byte[] key) {
    return ByteUtil.merge((date + "-").getBytes(), key);
  }

  private byte[] getCurrentDatePrefixKey(byte[] key) {
    return ByteUtil.merge(
        (DATE_FORMAT.format(dps.getLatestBlockHeaderTimestamp() + BLOCK_PRODUCED_INTERVAL) + "-")
            .getBytes(),
        key);
  }
}
