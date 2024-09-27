package org.tron.core.db;

import static org.tron.common.utils.Commons.adjustBalance;
import static org.tron.core.Constant.TRANSACTION_MAX_BYTE_SIZE;
import static org.tron.core.exception.BadBlockException.TypeEnum.CALC_MERKLE_ROOT_FAILED;
import static org.tron.protos.Protocol.Transaction.Contract.ContractType.TransferContract;
import static org.tron.protos.Protocol.Transaction.Result.contractResult.OUT_OF_ENERGY;
import static org.tron.protos.Protocol.Transaction.Result.contractResult.SUCCESS;
import static org.tron.protos.Protocol.TransactionInfo.code.SUCESS;

import com.alibaba.fastjson.JSONObject;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.prometheus.client.Histogram;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bouncycastle.util.encoders.Hex;
import org.quartz.CronExpression;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.api.GrpcAPI.TransactionInfoList;
import org.tron.common.args.GenesisBlock;
import org.tron.common.bloom.Bloom;
import org.tron.common.es.ExecutorServiceManager;
import org.tron.common.logsfilter.EventPluginLoader;
import org.tron.common.logsfilter.FilterQuery;
import org.tron.common.logsfilter.capsule.BlockFilterCapsule;
import org.tron.common.logsfilter.capsule.BlockLogTriggerCapsule;
import org.tron.common.logsfilter.capsule.ContractTriggerCapsule;
import org.tron.common.logsfilter.capsule.FilterTriggerCapsule;
import org.tron.common.logsfilter.capsule.LogsFilterCapsule;
import org.tron.common.logsfilter.capsule.SolidityTriggerCapsule;
import org.tron.common.logsfilter.capsule.TransactionLogTriggerCapsule;
import org.tron.common.logsfilter.capsule.TriggerCapsule;
import org.tron.common.logsfilter.trigger.ContractEventTrigger;
import org.tron.common.logsfilter.trigger.ContractLogTrigger;
import org.tron.common.logsfilter.trigger.ContractTrigger;
import org.tron.common.logsfilter.trigger.Trigger;
import org.tron.common.overlay.message.Message;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.prometheus.MetricKeys;
import org.tron.common.prometheus.MetricLabels;
import org.tron.common.prometheus.Metrics;
import org.tron.common.runtime.RuntimeImpl;
import org.tron.common.runtime.vm.LogInfo;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.ByteUtil;
import org.tron.common.utils.Commons;
import org.tron.common.utils.JsonUtil;
import org.tron.common.utils.Pair;
import org.tron.common.utils.SessionOptional;
import org.tron.common.utils.Sha256Hash;
import org.tron.common.utils.StringUtil;
import org.tron.common.zksnark.MerkleContainer;
import org.tron.consensus.Consensus;
import org.tron.consensus.base.Param.Miner;
import org.tron.core.ChainBaseManager;
import org.tron.core.Constant;
import org.tron.core.Wallet;
import org.tron.core.actuator.ActuatorCreator;
import org.tron.core.capsule.AccountCapsule;
import org.tron.core.capsule.BlockBalanceTraceCapsule;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.BlockCapsule.BlockId;
import org.tron.core.capsule.BytesCapsule;
import org.tron.core.capsule.ContractCapsule;
import org.tron.core.capsule.ContractStateCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.capsule.TransactionInfoCapsule;
import org.tron.core.capsule.TransactionRetCapsule;
import org.tron.core.capsule.UsdtTransferCapsule;
import org.tron.core.capsule.WitnessCapsule;
import org.tron.core.capsule.utils.TransactionUtil;
import org.tron.core.config.Parameter.ChainConstant;
import org.tron.core.config.args.Args;
import org.tron.core.consensus.ProposalController;
import org.tron.core.db.KhaosDatabase.KhaosBlock;
import org.tron.core.db.accountstate.TrieService;
import org.tron.core.db.accountstate.callback.AccountStateCallBack;
import org.tron.core.db.api.AssetUpdateHelper;
import org.tron.core.db.api.BandwidthPriceHistoryLoader;
import org.tron.core.db.api.EnergyPriceHistoryLoader;
import org.tron.core.db.api.MoveAbiHelper;
import org.tron.core.db.common.iterator.DBIterator;
import org.tron.core.db2.ISession;
import org.tron.core.db2.core.Chainbase;
import org.tron.core.db2.core.SnapshotManager;
import org.tron.core.exception.AccountResourceInsufficientException;
import org.tron.core.exception.BadBlockException;
import org.tron.core.exception.BadItemException;
import org.tron.core.exception.BadNumberBlockException;
import org.tron.core.exception.BalanceInsufficientException;
import org.tron.core.exception.ContractExeException;
import org.tron.core.exception.ContractSizeNotEqualToOneException;
import org.tron.core.exception.ContractValidateException;
import org.tron.core.exception.DupTransactionException;
import org.tron.core.exception.EventBloomException;
import org.tron.core.exception.ItemNotFoundException;
import org.tron.core.exception.NonCommonBlockException;
import org.tron.core.exception.ReceiptCheckErrException;
import org.tron.core.exception.TaposException;
import org.tron.core.exception.TooBigTransactionException;
import org.tron.core.exception.TooBigTransactionResultException;
import org.tron.core.exception.TransactionExpirationException;
import org.tron.core.exception.UnLinkedBlockException;
import org.tron.core.exception.VMIllegalException;
import org.tron.core.exception.ValidateScheduleException;
import org.tron.core.exception.ValidateSignatureException;
import org.tron.core.exception.ZksnarkException;
import org.tron.core.metrics.MetricsKey;
import org.tron.core.metrics.MetricsUtil;
import org.tron.core.service.MortgageService;
import org.tron.core.service.RewardViCalService;
import org.tron.core.services.http.NetUtil;
import org.tron.core.store.AccountAssetStore;
import org.tron.core.store.AccountIdIndexStore;
import org.tron.core.store.AccountIndexStore;
import org.tron.core.store.AccountStore;
import org.tron.core.store.AssetIssueStore;
import org.tron.core.store.AssetIssueV2Store;
import org.tron.core.store.CodeStore;
import org.tron.core.store.ContractStore;
import org.tron.core.store.DelegatedResourceAccountIndexStore;
import org.tron.core.store.DelegatedResourceStore;
import org.tron.core.store.DelegationStore;
import org.tron.core.store.DynamicPropertiesStore;
import org.tron.core.store.ExchangeStore;
import org.tron.core.store.ExchangeV2Store;
import org.tron.core.store.IncrementalMerkleTreeStore;
import org.tron.core.store.NullifierStore;
import org.tron.core.store.ProposalStore;
import org.tron.core.store.StorageRowStore;
import org.tron.core.store.StoreFactory;
import org.tron.core.store.TransactionHistoryStore;
import org.tron.core.store.TransactionRetStore;
import org.tron.core.store.VotesStore;
import org.tron.core.store.WitnessScheduleStore;
import org.tron.core.store.WitnessStore;
import org.tron.core.utils.TransactionRegister;
import org.tron.protos.Protocol;
import org.tron.protos.Protocol.AccountType;
import org.tron.protos.Protocol.Permission;
import org.tron.protos.Protocol.Transaction;
import org.tron.protos.Protocol.Transaction.Contract;
import org.tron.protos.Protocol.TransactionInfo;
import org.tron.protos.contract.BalanceContract;
import org.tron.protos.contract.SmartContractOuterClass;


@Slf4j(topic = "DB")
@Component
public class Manager {

  private static final int SHIELDED_TRANS_IN_BLOCK_COUNTS = 1;
  private static final String SAVE_BLOCK = "Save block: {}";
  private static final int SLEEP_TIME_OUT = 50;
  private static final int TX_ID_CACHE_SIZE = 100_000;
  private static final int SLEEP_FOR_WAIT_LOCK = 10;
  private static final int NO_BLOCK_WAITING_LOCK = 0;
  private final int shieldedTransInPendingMaxCounts =
      Args.getInstance().getShieldedTransInPendingMaxCounts();
  @Getter
  @Setter
  public boolean eventPluginLoaded = false;
  private int maxTransactionPendingSize = Args.getInstance().getMaxTransactionPendingSize();
  @Getter
  @Autowired
  private TransactionCache transactionCache;
  @Autowired
  private KhaosDatabase khaosDb;
  @Getter
  @Autowired
  private RevokingDatabase revokingStore;
  @Getter
  private SessionOptional session = SessionOptional.instance();
  @Getter
  @Setter
  private boolean isSyncMode;
  @Getter
  private Object forkLock = new Object();
  // map<Long, IncrementalMerkleTree>
  @Getter
  @Setter
  private String netType;
  @Getter
  @Setter
  private ProposalController proposalController;
  @Getter
  @Setter
  private MerkleContainer merkleContainer;
  private ExecutorService validateSignService;
  private String validateSignName = "validate-sign";
  private boolean isRunRePushThread = true;
  private boolean isRunTriggerCapsuleProcessThread = true;
  private BlockingQueue<TransactionCapsule> pushTransactionQueue = new LinkedBlockingQueue<>();
  @Getter
  private Cache<Sha256Hash, Boolean> transactionIdCache = CacheBuilder
      .newBuilder().maximumSize(TX_ID_CACHE_SIZE)
      .expireAfterWrite(1, TimeUnit.HOURS).recordStats().build();
  @Autowired
  private AccountStateCallBack accountStateCallBack;
  @Autowired
  private TrieService trieService;
  private Set<String> ownerAddressSet = new HashSet<>();
  @Getter
  @Autowired
  private MortgageService mortgageService;
  @Autowired
  private Consensus consensus;
  @Autowired
  @Getter
  private ChainBaseManager chainBaseManager;
  // transactions cache
  private BlockingQueue<TransactionCapsule> pendingTransactions;
  @Getter
  private AtomicInteger shieldedTransInPendingCounts = new AtomicInteger(0);
  // transactions popped
  private List<TransactionCapsule> poppedTransactions =
      Collections.synchronizedList(Lists.newArrayList());
  // the capacity is equal to Integer.MAX_VALUE default
  private BlockingQueue<TransactionCapsule> rePushTransactions;
  private BlockingQueue<TriggerCapsule> triggerCapsuleQueue;
  // log filter
  private boolean isRunFilterProcessThread = true;
  private BlockingQueue<FilterTriggerCapsule> filterCapsuleQueue;

  @Getter
  private volatile long latestSolidityNumShutDown;
  @Getter
  private long lastUsedSolidityNum = -1;
  @Getter
  private int maxFlushCount;

  @Getter
  private final ThreadLocal<Histogram.Timer> blockedTimer = new ThreadLocal<>();

  private AtomicInteger blockWaitLock = new AtomicInteger(0);
  private Object transactionLock = new Object();

  private ExecutorService rePushEs;
  private static final String rePushEsName = "repush";
  private ExecutorService triggerEs;
  private static final String triggerEsName = "event-trigger";
  private ExecutorService filterEs;
  private static final String filterEsName = "filter";
  private static final byte[] transferTopic =
      Hex.decode("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");
  private static final byte[] innerUsdtAddr =
      Hex.decode("a614f803B6FD780986A42c78Ec9c7f77e6DeD13C");
  private static final byte[] usdtAddr = Hex.decode("41a614f803B6FD780986A42c78Ec9c7f77e6DeD13C");
  private static Map<String, Set<byte[]>> cexAddrs;
  // Pump 发射后
  private static final byte[] targetMevAddress = Hex.decode("41987c0191a1A098Ffc9addC9C65d2c3d028B10CA3");
  private static AddrContinusRecord targetAddrContinusRecord = new AddrContinusRecord("");
  private static AddrAllInfoRecord targetAddrAllInfoRecord = new AddrAllInfoRecord("");
  private static final BigDecimal TRX_DIVISOR = new BigDecimal("1000000");
  private static final BigDecimal TOKEN_DIVISOR = new BigDecimal("1000000000000000000");
  private static final String SWAP_BUY_METHOD_1 = "fb3bdb41"; // swapETHForExactTokens
  private static final String SWAP_BUY_METHOD_2 = "7ff36ab5";
  private static final String SWAP_BUY_METHOD_3 =
      "b6f9de95"; // swapExactETHForTokensSupportingFeeOnTransferTokens
  private static final String SWAP_SELL_METHOD_1 = "18cbafe5";
  private static final String SWAP_SELL_METHOD_2 = "4a25d94a"; // swapTokensForExactETH
  private static final String SWAP_SELL_METHOD_3 =
      "791ac947"; // swapExactTokensForETHSupportingFeeOnTransferTokens
  private static final String SWAP_METHOD = "38ed1739";
  private static final byte[] PUMP_SWAP_ROUTER = Hex.decode("41fF7155b5df8008fbF3834922B2D52430b27874f5");
  private static final byte[] TRANSFER_TOPIC =
      Hex.decode("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");
  private static final byte[] SWAP_TOPIC =
      Hex.decode("d78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822");
  private static final byte[] WTRX_HEX = Hex.decode("891cdb91d149f23B1a45D9c5Ca78a88d0cB44C18");
  private static final String WTRX = "891cdb91d149f23B1a45D9c5Ca78a88d0cB44C18";
  private static final String WTRX41 = "41891cdb91d149f23B1a45D9c5Ca78a88d0cB44C18";
  private static final String USDT = "a614f803B6FD780986A42c78Ec9c7f77e6DeD13C".toLowerCase();
  private static final Map<String, String> pairToTokenMap = new HashMap<>();

  @Autowired
  private RewardViCalService rewardViCalService;

  /**
   * Cycle thread to rePush Transactions
   */
  private Runnable rePushLoop =
      () -> {
        while (isRunRePushThread) {
          TransactionCapsule tx = null;
          try {
            tx = getRePushTransactions().peek();
            if (tx != null) {
              this.rePush(tx);
            } else {
              TimeUnit.MILLISECONDS.sleep(SLEEP_TIME_OUT);
            }
          } catch (Throwable ex) {
            if (ex instanceof InterruptedException) {
              Thread.currentThread().interrupt();
            }
            logger.error("Unknown exception happened in rePush loop.", ex);
            if (tx != null) {
              Metrics.counterInc(MetricKeys.Counter.TXS, 1,
                  MetricLabels.Counter.TXS_FAIL, MetricLabels.Counter.TXS_FAIL_ERROR);
            }
          } finally {
            if (tx != null && getRePushTransactions().remove(tx)) {
              Metrics.gaugeInc(MetricKeys.Gauge.MANAGER_QUEUE, -1,
                  MetricLabels.Gauge.QUEUE_REPUSH);
            }
          }
        }
      };
  private Runnable triggerCapsuleProcessLoop =
      () -> {
        while (isRunTriggerCapsuleProcessThread) {
          try {
            TriggerCapsule triggerCapsule = triggerCapsuleQueue.poll(1, TimeUnit.SECONDS);
            if (triggerCapsule != null) {
              triggerCapsule.processTrigger();
            }
          } catch (InterruptedException ex) {
            logger.info(ex.getMessage());
            Thread.currentThread().interrupt();
          } catch (Throwable throwable) {
            logger.error("Unknown throwable happened in process capsule loop.", throwable);
          }
        }
      };

  private Runnable filterProcessLoop =
      () -> {
        while (isRunFilterProcessThread) {
          try {
            FilterTriggerCapsule filterCapsule = filterCapsuleQueue.poll(1, TimeUnit.SECONDS);
            if (filterCapsule != null) {
              filterCapsule.processFilterTrigger();
            }
          } catch (InterruptedException e) {
            logger.error("FilterProcessLoop get InterruptedException, error is {}.",
                    e.getMessage());
            Thread.currentThread().interrupt();
          } catch (Throwable throwable) {
            logger.error("Unknown throwable happened in filterProcessLoop. ", throwable);
          }
        }
      };

  private Comparator downComparator = (Comparator<TransactionCapsule>) (o1, o2) -> Long
      .compare(o2.getOrder(), o1.getOrder());

  public WitnessStore getWitnessStore() {
    return chainBaseManager.getWitnessStore();
  }

  public boolean needToUpdateAsset() {
    return getDynamicPropertiesStore().getTokenUpdateDone() == 0L;
  }

  public boolean needToMoveAbi() {
    return getDynamicPropertiesStore().getAbiMoveDone() == 0L;
  }

  private boolean needToLoadEnergyPriceHistory() {
    return getDynamicPropertiesStore().getEnergyPriceHistoryDone() == 0L;
  }

  private boolean needToLoadBandwidthPriceHistory() {
    return getDynamicPropertiesStore().getBandwidthPriceHistoryDone() == 0L;
  }

  public boolean needToSetBlackholePermission() {
    return getDynamicPropertiesStore().getSetBlackholeAccountPermission() == 0L;
  }

  private void resetBlackholeAccountPermission() {
    AccountCapsule blackholeAccount = getAccountStore().getBlackhole();

    byte[] zeroAddress = new byte[21];
    zeroAddress[0] = Wallet.getAddressPreFixByte();
    Permission owner = AccountCapsule
        .createDefaultOwnerPermission(ByteString.copyFrom(zeroAddress));
    blackholeAccount.updatePermissions(owner, null, null);
    getAccountStore().put(blackholeAccount.getAddress().toByteArray(), blackholeAccount);

    getDynamicPropertiesStore().saveSetBlackholePermission(1);
  }

  public DynamicPropertiesStore getDynamicPropertiesStore() {
    return chainBaseManager.getDynamicPropertiesStore();
  }

  public DelegationStore getDelegationStore() {
    return chainBaseManager.getDelegationStore();
  }

  public IncrementalMerkleTreeStore getMerkleTreeStore() {
    return chainBaseManager.getMerkleTreeStore();
  }

  public WitnessScheduleStore getWitnessScheduleStore() {
    return chainBaseManager.getWitnessScheduleStore();
  }

  public DelegatedResourceStore getDelegatedResourceStore() {
    return chainBaseManager.getDelegatedResourceStore();
  }

  public DelegatedResourceAccountIndexStore getDelegatedResourceAccountIndexStore() {
    return chainBaseManager.getDelegatedResourceAccountIndexStore();
  }

  public CodeStore getCodeStore() {
    return chainBaseManager.getCodeStore();
  }

  public ContractStore getContractStore() {
    return chainBaseManager.getContractStore();
  }

  public VotesStore getVotesStore() {
    return chainBaseManager.getVotesStore();
  }

  public ProposalStore getProposalStore() {
    return chainBaseManager.getProposalStore();
  }

  public ExchangeStore getExchangeStore() {
    return chainBaseManager.getExchangeStore();
  }

  public ExchangeV2Store getExchangeV2Store() {
    return chainBaseManager.getExchangeV2Store();
  }

  public StorageRowStore getStorageRowStore() {
    return chainBaseManager.getStorageRowStore();
  }

  public BlockIndexStore getBlockIndexStore() {
    return chainBaseManager.getBlockIndexStore();
  }

  public BlockingQueue<TransactionCapsule> getPendingTransactions() {
    return this.pendingTransactions;
  }

  public List<TransactionCapsule> getPoppedTransactions() {
    return this.poppedTransactions;
  }

  public BlockingQueue<TransactionCapsule> getRePushTransactions() {
    return rePushTransactions;
  }

  public void stopRePushThread() {
    isRunRePushThread = false;
    ExecutorServiceManager.shutdownAndAwaitTermination(rePushEs, rePushEsName);
  }

  public void stopRePushTriggerThread() {
    isRunTriggerCapsuleProcessThread = false;
    ExecutorServiceManager.shutdownAndAwaitTermination(triggerEs, triggerEsName);
  }

  public void stopFilterProcessThread() {
    isRunFilterProcessThread = false;
    ExecutorServiceManager.shutdownAndAwaitTermination(filterEs, filterEsName);
  }

  public void stopValidateSignThread() {
    ExecutorServiceManager.shutdownAndAwaitTermination(validateSignService, "validate-sign");
  }

  @PostConstruct
  public void init() {
    ChainBaseManager.init(chainBaseManager);
    Message.setDynamicPropertiesStore(this.getDynamicPropertiesStore());
    mortgageService
        .initStore(chainBaseManager.getWitnessStore(), chainBaseManager.getDelegationStore(),
            chainBaseManager.getDynamicPropertiesStore(), chainBaseManager.getAccountStore());
    accountStateCallBack.setChainBaseManager(chainBaseManager);
    trieService.setChainBaseManager(chainBaseManager);
    revokingStore.disable();
    revokingStore.check();
    transactionCache.initCache();
    rewardViCalService.init();
    this.setProposalController(ProposalController.createInstance(this));
    this.setMerkleContainer(
        merkleContainer.createInstance(chainBaseManager.getMerkleTreeStore(),
            chainBaseManager.getMerkleTreeIndexStore()));
    if (Args.getInstance().isOpenTransactionSort()) {
      this.pendingTransactions = new PriorityBlockingQueue(2000, downComparator);
      this.rePushTransactions = new PriorityBlockingQueue<>(2000, downComparator);
    } else {
      this.pendingTransactions = new LinkedBlockingQueue<>();
      this.rePushTransactions = new LinkedBlockingQueue<>();
    }
    this.triggerCapsuleQueue = new LinkedBlockingQueue<>();
    this.filterCapsuleQueue = new LinkedBlockingQueue<>();
    chainBaseManager.setMerkleContainer(getMerkleContainer());
    chainBaseManager.setMortgageService(mortgageService);
    this.initGenesis();
    try {
      this.khaosDb.start(chainBaseManager.getBlockById(
          getDynamicPropertiesStore().getLatestBlockHeaderHash()));
    } catch (ItemNotFoundException e) {
      logger.error(
          "Can not find Dynamic highest block from DB! \nnumber={} \nhash={}",
          getDynamicPropertiesStore().getLatestBlockHeaderNumber(),
          getDynamicPropertiesStore().getLatestBlockHeaderHash());
      logger.error(
          "Please delete database directory({}) and restart",
          Args.getInstance().getOutputDirectory());
      System.exit(1);
    } catch (BadItemException e) {
      logger.error("DB data broken {}.", e.getMessage());
      logger.error(
          "Please delete database directory({}) and restart.",
          Args.getInstance().getOutputDirectory());
      System.exit(1);
    }
    getChainBaseManager().getForkController().init(this.chainBaseManager);

    if (Args.getInstance().isNeedToUpdateAsset() && needToUpdateAsset()) {
      new AssetUpdateHelper(chainBaseManager).doWork();
    }

    if (needToMoveAbi()) {
      new MoveAbiHelper(chainBaseManager).doWork();
    }

    if (needToLoadEnergyPriceHistory()) {
      new EnergyPriceHistoryLoader(chainBaseManager).doWork();
    }

    if (needToLoadBandwidthPriceHistory()) {
      new BandwidthPriceHistoryLoader(chainBaseManager).doWork();
    }

    if (needToSetBlackholePermission()) {
      resetBlackholeAccountPermission();
    }

    //for test only
    chainBaseManager.getDynamicPropertiesStore().updateDynamicStoreByConfig();

    // init liteFullNode
    initLiteNode();

    long headNum = chainBaseManager.getDynamicPropertiesStore().getLatestBlockHeaderNumber();
    logger.info("Current headNum is: {}.", headNum);
    boolean isLite = chainBaseManager.isLiteNode();
    logger.info("Node type is: {}.", isLite ? "lite" : "full");
    if (isLite) {
      logger.info("Lite node lowestNum: {}", chainBaseManager.getLowestBlockNum());
    }
    revokingStore.enable();
    validateSignService = ExecutorServiceManager
        .newFixedThreadPool(validateSignName, Args.getInstance().getValidateSignThreadNum());
    rePushEs = ExecutorServiceManager.newSingleThreadExecutor(rePushEsName, true);
    rePushEs.submit(rePushLoop);
    // add contract event listener for subscribing
    if (Args.getInstance().isEventSubscribe()) {
      startEventSubscribing();
      triggerEs = ExecutorServiceManager.newSingleThreadExecutor(triggerEsName, true);
      triggerEs.submit(triggerCapsuleProcessLoop);
    }

    // start json rpc filter process
    if (CommonParameter.getInstance().isJsonRpcFilterEnabled()) {
      filterEs = ExecutorServiceManager.newSingleThreadExecutor(filterEsName);
      filterEs.submit(filterProcessLoop);
    }

    //initStoreFactory
    prepareStoreFactory();
    //initActuatorCreator
    ActuatorCreator.init();
    TransactionRegister.registerActuator();
    // init auto-stop
    try {
      initAutoStop();
    } catch (IllegalArgumentException e) {
      logger.error("Auto-stop params error: {}", e.getMessage());
      System.exit(1);
    }

    maxFlushCount = CommonParameter.getInstance().getStorage().getMaxFlushCount();

    cexAddrs = getTronCexAddresses();
  }

  private static Map<String, Set<byte[]>> getTronCexAddresses() {
    try {
      JSONObject resObject =
          JSONObject.parseObject(NetUtil.get("https://apilist.tronscanapi.com/api/hot/exchanges"));

      Map<String, Set<byte[]>> res = new HashMap<>();

      resObject
          .getJSONArray("exchanges")
          .forEach(
              obj -> {
                JSONObject jo = (JSONObject) obj;
                String addr = jo.getString("address");
                String name = jo.getString("name");
                String cexName;
                if (name.contains("Binance") || name.contains("binance")) {
                  cexName = "Binance";
                } else if (name.contains("Okex") || name.contains("okex")) {
                  cexName = "Okex";
                } else if (name.contains("bybit") || name.contains("Bybit")) {
                  cexName = "Bybit";
                } else {
                  cexName = "Others";
                }
                Set<byte[]> addrs = res.getOrDefault(cexName, new HashSet<>());
                addrs.add(Commons.decodeFromBase58Check(addr));
                res.put(cexName, addrs);
              });
      return res;
    } catch (Exception e) {
      throw e;
    }
  }

  /**
   * init genesis block.
   */
  public void initGenesis() {
    chainBaseManager.initGenesis();
    BlockCapsule genesisBlock = chainBaseManager.getGenesisBlock();

    if (chainBaseManager.containBlock(genesisBlock.getBlockId())) {
      Args.getInstance().setChainId(genesisBlock.getBlockId().toString());
    } else {
      if (chainBaseManager.hasBlocks()) {
        logger.error(
            "Genesis block modify, please delete database directory({}) and restart.",
            Args.getInstance().getOutputDirectory());
        System.exit(1);
      } else {
        logger.info("Create genesis block.");
        Args.getInstance().setChainId(genesisBlock.getBlockId().toString());

        chainBaseManager.getBlockStore().put(genesisBlock.getBlockId().getBytes(), genesisBlock);
        chainBaseManager.getBlockIndexStore().put(genesisBlock.getBlockId());

        logger.info(SAVE_BLOCK, genesisBlock);
        // init Dynamic Properties Store
        chainBaseManager.getDynamicPropertiesStore().saveLatestBlockHeaderNumber(0);
        chainBaseManager.getDynamicPropertiesStore().saveLatestBlockHeaderHash(
            genesisBlock.getBlockId().getByteString());
        chainBaseManager.getDynamicPropertiesStore().saveLatestBlockHeaderTimestamp(
            genesisBlock.getTimeStamp());
        this.initAccount();
        this.initWitness();
        this.khaosDb.start(genesisBlock);
        this.updateRecentBlock(genesisBlock);
        initAccountHistoryBalance();
      }
    }
  }

  /**
   * save account into database.
   */
  public void initAccount() {
    final CommonParameter parameter = CommonParameter.getInstance();
    final GenesisBlock genesisBlockArg = parameter.getGenesisBlock();
    genesisBlockArg
        .getAssets()
        .forEach(
            account -> {
              account.setAccountType("Normal"); // to be set in conf
              final AccountCapsule accountCapsule =
                  new AccountCapsule(
                      account.getAccountName(),
                      ByteString.copyFrom(account.getAddress()),
                      account.getAccountType(),
                      account.getBalance());
              chainBaseManager.getAccountStore().put(account.getAddress(), accountCapsule);
              chainBaseManager.getAccountIdIndexStore().put(accountCapsule);
              chainBaseManager.getAccountIndexStore().put(accountCapsule);
            });
  }

  public void initAccountHistoryBalance() {
    BlockCapsule genesis = chainBaseManager.getGenesisBlock();
    BlockBalanceTraceCapsule genesisBlockBalanceTraceCapsule =
        new BlockBalanceTraceCapsule(genesis);
    List<TransactionCapsule> transactionCapsules = genesis.getTransactions();
    for (TransactionCapsule transactionCapsule : transactionCapsules) {
      BalanceContract.TransferContract transferContract = transactionCapsule.getTransferContract();
      BalanceContract.TransactionBalanceTrace.Operation operation =
          BalanceContract.TransactionBalanceTrace.Operation.newBuilder()
              .setOperationIdentifier(0)
              .setAddress(transferContract.getToAddress())
              .setAmount(transferContract.getAmount())
              .build();

      BalanceContract.TransactionBalanceTrace transactionBalanceTrace =
          BalanceContract.TransactionBalanceTrace.newBuilder()
              .setTransactionIdentifier(transactionCapsule.getTransactionId().getByteString())
              .setType(TransferContract.name())
              .setStatus(SUCCESS.name())
              .addOperation(operation)
              .build();
      genesisBlockBalanceTraceCapsule.addTransactionBalanceTrace(transactionBalanceTrace);

      chainBaseManager.getAccountTraceStore().recordBalanceWithBlock(
          transferContract.getToAddress().toByteArray(), 0, transferContract.getAmount());
    }

    chainBaseManager.getBalanceTraceStore()
        .put(Longs.toByteArray(0), genesisBlockBalanceTraceCapsule);
  }

  /**
   * save witnesses into database.
   */
  private void initWitness() {
    final CommonParameter commonParameter = Args.getInstance();
    final GenesisBlock genesisBlockArg = commonParameter.getGenesisBlock();
    genesisBlockArg
        .getWitnesses()
        .forEach(
            key -> {
              byte[] keyAddress = key.getAddress();
              ByteString address = ByteString.copyFrom(keyAddress);

              final AccountCapsule accountCapsule;
              if (!chainBaseManager.getAccountStore().has(keyAddress)) {
                accountCapsule = new AccountCapsule(ByteString.EMPTY,
                    address, AccountType.AssetIssue, 0L);
              } else {
                accountCapsule = chainBaseManager.getAccountStore().getUnchecked(keyAddress);
              }
              accountCapsule.setIsWitness(true);
              chainBaseManager.getAccountStore().put(keyAddress, accountCapsule);

              final WitnessCapsule witnessCapsule =
                  new WitnessCapsule(address, key.getVoteCount(), key.getUrl());
              witnessCapsule.setIsJobs(true);
              chainBaseManager.getWitnessStore().put(keyAddress, witnessCapsule);
            });
  }

  /**
   * init auto-stop, check params
   */
  private void initAutoStop() {
    final long headNum = chainBaseManager.getHeadBlockNum();
    final long headTime = chainBaseManager.getHeadBlockTimeStamp();
    final long exitHeight = CommonParameter.getInstance().getShutdownBlockHeight();
    final long exitCount = CommonParameter.getInstance().getShutdownBlockCount();
    final CronExpression blockTime = Args.getInstance().getShutdownBlockTime();

    if (exitHeight > 0 && exitHeight < headNum) {
      throw new IllegalArgumentException(
          String.format("shutDownBlockHeight %d is less than headNum %d", exitHeight, headNum));
    }

    if (exitCount == 0) {
      throw new IllegalArgumentException(
          String.format("shutDownBlockCount %d is less than 1", exitCount));
    }

    if (blockTime != null && blockTime.getNextValidTimeAfter(new Date(headTime)) == null) {
      throw new IllegalArgumentException(
          String.format("shutDownBlockTime %s is illegal", blockTime));
    }

    if (exitHeight > 0 && exitCount > 0) {
      throw new IllegalArgumentException(
          String.format("shutDownBlockHeight %d and shutDownBlockCount %d set both",
              exitHeight, exitCount));
    }

    if (exitHeight > 0 && blockTime != null) {
      throw new IllegalArgumentException(
          String.format("shutDownBlockHeight %d and shutDownBlockTime %s set both",
              exitHeight, blockTime));
    }

    if (exitCount > 0 && blockTime != null) {
      throw new IllegalArgumentException(
          String.format("shutDownBlockCount %d and shutDownBlockTime %s set both",
              exitCount, blockTime));
    }

    if (exitHeight == headNum && (!Args.getInstance().isP2pDisable())) {
      logger.info("Auto-stop hit: shutDownBlockHeight: {}, currentHeaderNum: {}, exit now",
          exitHeight, headNum);
      System.exit(0);
    }

    if (exitCount > 0) {
      CommonParameter.getInstance().setShutdownBlockHeight(headNum + exitCount);
    }
    // init
    latestSolidityNumShutDown = CommonParameter.getInstance().getShutdownBlockHeight();
  }

  public AccountStore getAccountStore() {
    return chainBaseManager.getAccountStore();
  }

  public AccountAssetStore getAccountAssetStore() {
    return chainBaseManager.getAccountAssetStore();
  }

  public AccountIndexStore getAccountIndexStore() {
    return chainBaseManager.getAccountIndexStore();
  }

  void validateTapos(TransactionCapsule transactionCapsule) throws TaposException {
    byte[] refBlockHash = transactionCapsule.getInstance()
        .getRawData().getRefBlockHash().toByteArray();
    byte[] refBlockNumBytes = transactionCapsule.getInstance()
        .getRawData().getRefBlockBytes().toByteArray();
    try {
      byte[] blockHash = chainBaseManager.getRecentBlockStore().get(refBlockNumBytes).getData();
      if (!Arrays.equals(blockHash, refBlockHash)) {
        String str = String.format(
            "Tapos failed, different block hash, %s, %s , recent block %s, "
                + "solid block %s head block %s",
            ByteArray.toLong(refBlockNumBytes), Hex.toHexString(refBlockHash),
            Hex.toHexString(blockHash),
            chainBaseManager.getSolidBlockId().getString(),
            chainBaseManager.getHeadBlockId().getString());
        throw new TaposException(str);
      }
    } catch (ItemNotFoundException e) {
      String str = String
          .format("Tapos failed, block not found, ref block %s, %s , solid block %s head block %s",
              ByteArray.toLong(refBlockNumBytes), Hex.toHexString(refBlockHash),
              chainBaseManager.getSolidBlockId().getString(),
              chainBaseManager.getHeadBlockId().getString());
      throw new TaposException(str);
    }
  }

  void validateCommon(TransactionCapsule transactionCapsule)
      throws TransactionExpirationException, TooBigTransactionException {
    if (!transactionCapsule.isInBlock()) {
      transactionCapsule.removeRedundantRet();
      long generalBytesSize =
          transactionCapsule.getInstance().toBuilder().clearRet().build().getSerializedSize()
              + Constant.MAX_RESULT_SIZE_IN_TX + Constant.MAX_RESULT_SIZE_IN_TX;
      if (generalBytesSize > TRANSACTION_MAX_BYTE_SIZE) {
        throw new TooBigTransactionException(String.format(
            "Too big transaction with result, TxId %s, the size is %d bytes, maxTxSize %d",
            transactionCapsule.getTransactionId(), generalBytesSize, TRANSACTION_MAX_BYTE_SIZE));
      }
    }
    if (transactionCapsule.getData().length > Constant.TRANSACTION_MAX_BYTE_SIZE) {
      throw new TooBigTransactionException(String.format(
          "Too big transaction, TxId %s, the size is %d bytes, maxTxSize %d",
          transactionCapsule.getTransactionId(), transactionCapsule.getData().length,
          TRANSACTION_MAX_BYTE_SIZE));
    }
    long transactionExpiration = transactionCapsule.getExpiration();
    long headBlockTime = chainBaseManager.getHeadBlockTimeStamp();
    if (transactionExpiration <= headBlockTime
        || transactionExpiration > headBlockTime + Constant.MAXIMUM_TIME_UNTIL_EXPIRATION) {
      throw new TransactionExpirationException(
          String.format(
          "Transaction expiration, transaction expiration time is %d, but headBlockTime is %d",
              transactionExpiration, headBlockTime));
    }
  }

  void validateDup(TransactionCapsule transactionCapsule) throws DupTransactionException {
    if (containsTransaction(transactionCapsule)) {
      throw new DupTransactionException(String.format("dup trans : %s ",
          transactionCapsule.getTransactionId()));
    }
  }

  private boolean containsTransaction(TransactionCapsule transactionCapsule) {
    return containsTransaction(transactionCapsule.getTransactionId().getBytes());
  }


  private boolean containsTransaction(byte[] transactionId) {
    if (transactionCache != null && !transactionCache.has(transactionId)) {
      // using the bloom filter only determines non-existent transaction
      return false;
    }

    return chainBaseManager.getTransactionStore()
        .has(transactionId);
  }

  /**
   * push transaction into pending.
   */
  public boolean pushTransaction(final TransactionCapsule trx)
      throws ValidateSignatureException, ContractValidateException, ContractExeException,
      AccountResourceInsufficientException, DupTransactionException, TaposException,
      TooBigTransactionException, TransactionExpirationException,
      ReceiptCheckErrException, VMIllegalException, TooBigTransactionResultException {

    if (isShieldedTransaction(trx.getInstance()) && !Args.getInstance()
        .isFullNodeAllowShieldedTransactionArgs()) {
      return true;
    }

    pushTransactionQueue.add(trx);
    Metrics.gaugeInc(MetricKeys.Gauge.MANAGER_QUEUE, 1,
        MetricLabels.Gauge.QUEUE_QUEUED);
    try {
      if (!trx.validateSignature(chainBaseManager.getAccountStore(),
          chainBaseManager.getDynamicPropertiesStore())) {
        throw new ValidateSignatureException(String.format("trans sig validate failed, id: %s",
            trx.getTransactionId()));
      }

      synchronized (transactionLock) {
        while (true) {
          try {
            if (isBlockWaitingLock()) {
              TimeUnit.MILLISECONDS.sleep(SLEEP_FOR_WAIT_LOCK);
            } else {
              break;
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.debug("The wait has been interrupted.");
          }
        }
        synchronized (this) {
          if (isShieldedTransaction(trx.getInstance())
                  && shieldedTransInPendingCounts.get() >= shieldedTransInPendingMaxCounts) {
            return false;
          }
          if (!session.valid()) {
            session.setValue(revokingStore.buildSession());
          }

          try (ISession tmpSession = revokingStore.buildSession()) {
            processTransaction(trx, null, false);
            trx.setTrxTrace(null);
            pendingTransactions.add(trx);
            Metrics.gaugeInc(MetricKeys.Gauge.MANAGER_QUEUE, 1,
                    MetricLabels.Gauge.QUEUE_PENDING);
            tmpSession.merge();
          }
          if (isShieldedTransaction(trx.getInstance())) {
            shieldedTransInPendingCounts.incrementAndGet();
          }
        }
      }
    } finally {
      if (pushTransactionQueue.remove(trx)) {
        Metrics.gaugeInc(MetricKeys.Gauge.MANAGER_QUEUE, -1,
            MetricLabels.Gauge.QUEUE_QUEUED);
      }
    }
    return true;
  }

  public void consumeMultiSignFee(TransactionCapsule trx, TransactionTrace trace)
      throws AccountResourceInsufficientException {
    if (trx.getInstance().getSignatureCount() > 1) {
      long fee = getDynamicPropertiesStore().getMultiSignFee();

      List<Contract> contracts = trx.getInstance().getRawData().getContractList();
      for (Contract contract : contracts) {
        byte[] address = TransactionCapsule.getOwner(contract);
        AccountCapsule accountCapsule = getAccountStore().get(address);
        try {
          if (accountCapsule != null) {
            adjustBalance(getAccountStore(), accountCapsule, -fee);

            if (getDynamicPropertiesStore().supportBlackHoleOptimization()) {
              getDynamicPropertiesStore().burnTrx(fee);
            } else {
              adjustBalance(getAccountStore(), this.getAccountStore().getBlackhole(), +fee);
            }
          }
        } catch (BalanceInsufficientException e) {
          throw new AccountResourceInsufficientException(
              String.format("account %s insufficient balance[%d] to multiSign",
                  StringUtil.encode58Check(address), fee));
        }
      }

      trace.getReceipt().setMultiSignFee(fee);
    }
  }

  public void consumeMemoFee(TransactionCapsule trx, TransactionTrace trace)
      throws AccountResourceInsufficientException {
    if (trx.getInstance().getRawData().getData().isEmpty()) {
      // no memo
      return;
    }

    long fee = getDynamicPropertiesStore().getMemoFee();
    if (fee == 0) {
      return;
    }

    List<Contract> contracts = trx.getInstance().getRawData().getContractList();
    for (Contract contract : contracts) {
      byte[] address = TransactionCapsule.getOwner(contract);
      AccountCapsule accountCapsule = getAccountStore().get(address);
      try {
        if (accountCapsule != null) {
          adjustBalance(getAccountStore(), accountCapsule, -fee);

          if (getDynamicPropertiesStore().supportBlackHoleOptimization()) {
            getDynamicPropertiesStore().burnTrx(fee);
          } else {
            adjustBalance(getAccountStore(), this.getAccountStore().getBlackhole(), +fee);
          }
        }
      } catch (BalanceInsufficientException e) {
        throw new AccountResourceInsufficientException(
            String.format("account %s insufficient balance[%d] to memo fee",
                StringUtil.encode58Check(address), fee));
      }
    }

    trace.getReceipt().setMemoFee(fee);
  }

  public void consumeBandwidth(TransactionCapsule trx, TransactionTrace trace)
      throws ContractValidateException, AccountResourceInsufficientException,
      TooBigTransactionResultException, TooBigTransactionException {
    BandwidthProcessor processor = new BandwidthProcessor(chainBaseManager);
    processor.consume(trx, trace);
  }


  /**
   * when switch fork need erase blocks on fork branch.
   */
  public void eraseBlock() {
    session.reset();
    try {
      BlockCapsule oldHeadBlock = chainBaseManager.getBlockById(
          getDynamicPropertiesStore().getLatestBlockHeaderHash());
      logger.info("Start to erase block: {}.", oldHeadBlock);
      khaosDb.pop();
      revokingStore.fastPop();
      logger.info("End to erase block: {}.", oldHeadBlock);
      oldHeadBlock.getTransactions().forEach(tc ->
          poppedTransactions.add(new TransactionCapsule(tc.getInstance())));
      Metrics.gaugeInc(MetricKeys.Gauge.MANAGER_QUEUE, oldHeadBlock.getTransactions().size(),
          MetricLabels.Gauge.QUEUE_POPPED);

    } catch (ItemNotFoundException | BadItemException e) {
      logger.warn(e.getMessage(), e);
    }
  }

  public void pushVerifiedBlock(BlockCapsule block) throws ContractValidateException,
      ContractExeException, ValidateSignatureException, AccountResourceInsufficientException,
      TransactionExpirationException, TooBigTransactionException, DupTransactionException,
      TaposException, ValidateScheduleException, ReceiptCheckErrException,
      VMIllegalException, TooBigTransactionResultException, UnLinkedBlockException,
      NonCommonBlockException, BadNumberBlockException, BadBlockException, ZksnarkException,
      EventBloomException {
    block.generatedByMyself = true;
    long start = System.currentTimeMillis();
    pushBlock(block);
    logger.info("Push block cost: {} ms, blockNum: {}, blockHash: {}, trx count: {}.",
        System.currentTimeMillis() - start,
        block.getNum(),
        block.getBlockId(),
        block.getTransactions().size());
  }

  private void applyBlock(BlockCapsule block) throws ContractValidateException,
      ContractExeException, ValidateSignatureException, AccountResourceInsufficientException,
      TransactionExpirationException, TooBigTransactionException, DupTransactionException,
      TaposException, ValidateScheduleException, ReceiptCheckErrException,
      VMIllegalException, TooBigTransactionResultException,
      ZksnarkException, BadBlockException, EventBloomException {
    applyBlock(block, block.getTransactions());
  }

  private void applyBlock(BlockCapsule block, List<TransactionCapsule> txs)
      throws ContractValidateException, ContractExeException, ValidateSignatureException,
      AccountResourceInsufficientException, TransactionExpirationException,
      TooBigTransactionException, DupTransactionException, TaposException,
      ValidateScheduleException, ReceiptCheckErrException, VMIllegalException,
      TooBigTransactionResultException, ZksnarkException, BadBlockException, EventBloomException {
    processBlock(block, txs);
    chainBaseManager.getBlockStore().put(block.getBlockId().getBytes(), block);
    chainBaseManager.getBlockIndexStore().put(block.getBlockId());
    if (block.getTransactions().size() != 0) {
      chainBaseManager.getTransactionRetStore()
          .put(ByteArray.fromLong(block.getNum()), block.getResult());
    }

    updateFork(block);
    if (System.currentTimeMillis() - block.getTimeStamp() >= 60_000) {
      revokingStore.setMaxFlushCount(maxFlushCount);
      if (Args.getInstance().getShutdownBlockTime() != null
          && Args.getInstance().getShutdownBlockTime().getNextValidTimeAfter(
            new Date(block.getTimeStamp() - maxFlushCount * 1000 * 3L))
          .compareTo(new Date(block.getTimeStamp())) <= 0) {
        revokingStore.setMaxFlushCount(SnapshotManager.DEFAULT_MIN_FLUSH_COUNT);
      }
      if (latestSolidityNumShutDown > 0 && latestSolidityNumShutDown - block.getNum()
          <= maxFlushCount) {
        revokingStore.setMaxFlushCount(SnapshotManager.DEFAULT_MIN_FLUSH_COUNT);
      }
    } else {
      revokingStore.setMaxFlushCount(SnapshotManager.DEFAULT_MIN_FLUSH_COUNT);
    }
  }

  private void switchFork(BlockCapsule newHead)
      throws ValidateSignatureException, ContractValidateException, ContractExeException,
      ValidateScheduleException, AccountResourceInsufficientException, TaposException,
      TooBigTransactionException, TooBigTransactionResultException, DupTransactionException,
      TransactionExpirationException, NonCommonBlockException, ReceiptCheckErrException,
      VMIllegalException, ZksnarkException, BadBlockException, EventBloomException {

    MetricsUtil.meterMark(MetricsKey.BLOCKCHAIN_FORK_COUNT);
    Metrics.counterInc(MetricKeys.Counter.BLOCK_FORK, 1, MetricLabels.ALL);

    Pair<LinkedList<KhaosBlock>, LinkedList<KhaosBlock>> binaryTree;
    try {
      binaryTree =
          khaosDb.getBranch(
              newHead.getBlockId(), getDynamicPropertiesStore().getLatestBlockHeaderHash());
    } catch (NonCommonBlockException e) {
      Metrics.counterInc(MetricKeys.Counter.BLOCK_FORK, 1, MetricLabels.FAIL);
      MetricsUtil.meterMark(MetricsKey.BLOCKCHAIN_FAIL_FORK_COUNT);
      logger.info(
          "This is not the most recent common ancestor, "
              + "need to remove all blocks in the fork chain.");
      BlockCapsule tmp = newHead;
      while (tmp != null) {
        khaosDb.removeBlk(tmp.getBlockId());
        tmp = khaosDb.getBlock(tmp.getParentHash());
      }

      throw e;
    }

    if (CollectionUtils.isNotEmpty(binaryTree.getValue())) {
      while (!getDynamicPropertiesStore()
          .getLatestBlockHeaderHash()
          .equals(binaryTree.getValue().peekLast().getParentHash())) {
        reOrgContractTrigger();
        reOrgLogsFilter();
        eraseBlock();
      }
    }

    if (CollectionUtils.isNotEmpty(binaryTree.getKey())) {
      List<KhaosBlock> first = new ArrayList<>(binaryTree.getKey());
      Collections.reverse(first);
      for (KhaosBlock item : first) {
        Exception exception = null;
        // todo  process the exception carefully later
        try (ISession tmpSession = revokingStore.buildSession()) {
          applyBlock(item.getBlk().setSwitch(true));
          tmpSession.commit();
        } catch (AccountResourceInsufficientException
            | ValidateSignatureException
            | ContractValidateException
            | ContractExeException
            | TaposException
            | DupTransactionException
            | TransactionExpirationException
            | ReceiptCheckErrException
            | TooBigTransactionException
            | TooBigTransactionResultException
            | ValidateScheduleException
            | VMIllegalException
            | ZksnarkException
            | BadBlockException e) {
          logger.warn(e.getMessage(), e);
          exception = e;
          throw e;
        } finally {
          if (exception != null) {
            Metrics.counterInc(MetricKeys.Counter.BLOCK_FORK, 1, MetricLabels.FAIL);
            MetricsUtil.meterMark(MetricsKey.BLOCKCHAIN_FAIL_FORK_COUNT);
            logger.warn("Switch back because exception thrown while switching forks.", exception);
            first.forEach(khaosBlock -> khaosDb.removeBlk(khaosBlock.getBlk().getBlockId()));
            khaosDb.setHead(binaryTree.getValue().peekFirst());

            while (!getDynamicPropertiesStore()
                .getLatestBlockHeaderHash()
                .equals(binaryTree.getValue().peekLast().getParentHash())) {
              eraseBlock();
            }

            List<KhaosBlock> second = new ArrayList<>(binaryTree.getValue());
            Collections.reverse(second);
            for (KhaosBlock khaosBlock : second) {
              // todo  process the exception carefully later
              try (ISession tmpSession = revokingStore.buildSession()) {
                applyBlock(khaosBlock.getBlk().setSwitch(true));
                tmpSession.commit();
              } catch (AccountResourceInsufficientException
                  | ValidateSignatureException
                  | ContractValidateException
                  | ContractExeException
                  | TaposException
                  | DupTransactionException
                  | TransactionExpirationException
                  | TooBigTransactionException
                  | ValidateScheduleException
                  | ZksnarkException e) {
                logger.warn(e.getMessage(), e);
              }
            }
          }
        }
      }
    }

  }

  public List<TransactionCapsule> getVerifyTxs(BlockCapsule block) {

    if (pendingTransactions.size() == 0) {
      return block.getTransactions();
    }

    List<TransactionCapsule> txs = new ArrayList<>();
    Set<String> txIds = new HashSet<>();
    Set<String> multiAddresses = new HashSet<>();

    pendingTransactions.forEach(capsule -> {
      String txId = Hex.toHexString(capsule.getTransactionId().getBytes());
      if (isMultiSignTransaction(capsule.getInstance())) {
        String address = Hex.toHexString(capsule.getOwnerAddress());
        multiAddresses.add(address);
      } else {
        txIds.add(txId);
      }
    });

    block.getTransactions().forEach(capsule -> {
      String address = Hex.toHexString(capsule.getOwnerAddress());
      String txId = Hex.toHexString(capsule.getTransactionId().getBytes());
      if (multiAddresses.contains(address) || !txIds.contains(txId)) {
        txs.add(capsule);
      } else {
        capsule.setVerified(true);
      }
    });

    return txs;
  }

  /**
   * save a block.
   */
  public void pushBlock(final BlockCapsule block)
      throws ValidateSignatureException, ContractValidateException, ContractExeException,
      UnLinkedBlockException, ValidateScheduleException, AccountResourceInsufficientException,
      TaposException, TooBigTransactionException, TooBigTransactionResultException,
      DupTransactionException, TransactionExpirationException,
      BadNumberBlockException, BadBlockException, NonCommonBlockException,
      ReceiptCheckErrException, VMIllegalException, ZksnarkException, EventBloomException {
    setBlockWaitLock(true);
    try {
      synchronized (this) {
        Metrics.histogramObserve(blockedTimer.get());
        blockedTimer.remove();
        long headerNumber = getDynamicPropertiesStore().getLatestBlockHeaderNumber();
        if (block.getNum() <= headerNumber && khaosDb.containBlockInMiniStore(block.getBlockId())) {
          logger.info("Block {} is already exist.", block.getBlockId().getString());
          return;
        }
        final Histogram.Timer timer = Metrics.histogramStartTimer(
                MetricKeys.Histogram.BLOCK_PUSH_LATENCY);
        long start = System.currentTimeMillis();
        List<TransactionCapsule> txs = getVerifyTxs(block);
        logger.info("Block num: {}, re-push-size: {}, pending-size: {}, "
                        + "block-tx-size: {}, verify-tx-size: {}",
                block.getNum(), rePushTransactions.size(), pendingTransactions.size(),
                block.getTransactions().size(), txs.size());

        if (CommonParameter.getInstance().getShutdownBlockTime() != null
                && CommonParameter.getInstance().getShutdownBlockTime()
                .isSatisfiedBy(new Date(block.getTimeStamp()))) {
          latestSolidityNumShutDown = block.getNum();
        }

        try (PendingManager pm = new PendingManager(this)) {

          if (!block.generatedByMyself) {
            if (!block.calcMerkleRoot().equals(block.getMerkleRoot())) {
              logger.warn("Num: {}, the merkle root doesn't match, expect is {} , actual is {}.",
                  block.getNum(), block.getMerkleRoot(), block.calcMerkleRoot());
              throw new BadBlockException(CALC_MERKLE_ROOT_FAILED,
                      String.format("The merkle hash is not validated for %d", block.getNum()));
            }
            consensus.receiveBlock(block);
          }

          if (block.getTransactions().stream()
                  .filter(tran -> isShieldedTransaction(tran.getInstance()))
                  .count() > SHIELDED_TRANS_IN_BLOCK_COUNTS) {
            throw new BadBlockException(
                String.format("num: %d, shielded transaction count > %d",
                    block.getNum(), SHIELDED_TRANS_IN_BLOCK_COUNTS));
          }

          BlockCapsule newBlock;
          try {
            newBlock = this.khaosDb.push(block);
          } catch (UnLinkedBlockException e) {
            logger.error(
                    "LatestBlockHeaderHash: {}, latestBlockHeaderNumber: {}"
                            + ", latestSolidifiedBlockNum: {}.",
                    getDynamicPropertiesStore().getLatestBlockHeaderHash(),
                    getDynamicPropertiesStore().getLatestBlockHeaderNumber(),
                    getDynamicPropertiesStore().getLatestSolidifiedBlockNum());
            throw e;
          }

          // DB don't need lower block
          if (getDynamicPropertiesStore().getLatestBlockHeaderHash() == null) {
            if (newBlock.getNum() != 0) {
              return;
            }
          } else {
            if (newBlock.getNum() <= headerNumber) {
              return;
            }

            // switch fork
            if (!newBlock
                    .getParentHash()
                    .equals(getDynamicPropertiesStore().getLatestBlockHeaderHash())) {
              logger.warn("Switch fork! new head num = {}, block id = {}.",
                      newBlock.getNum(), newBlock.getBlockId());

              logger.warn(
                      "******** Before switchFork ******* push block: {}, new block: {}, "
                          + "dynamic head num: {}, dynamic head hash: {}, "
                          + "dynamic head timestamp: {}, khaosDb head: {}, "
                          + "khaosDb miniStore size: {}, khaosDb unlinkMiniStore size: {}.",
                  block, newBlock,
                  chainBaseManager.getDynamicPropertiesStore().getLatestBlockHeaderNumber(),
                  chainBaseManager.getDynamicPropertiesStore().getLatestBlockHeaderHash(),
                  chainBaseManager.getDynamicPropertiesStore().getLatestBlockHeaderTimestamp(),
                  khaosDb.getHead(), khaosDb.getMiniStore().size(),
                  khaosDb.getMiniUnlinkedStore().size());
              synchronized (forkLock) {
                switchFork(newBlock);
              }
              logger.info(SAVE_BLOCK, newBlock);

              logger.warn(
                  "******** After switchFork ******* push block: {}, new block: {}, "
                      + "dynamic head num: {}, dynamic head hash: {}, "
                      + "dynamic head timestamp: {}, khaosDb head: {}, "
                      + "khaosDb miniStore size: {}, khaosDb unlinkMiniStore size: {}.",
                  block, newBlock,
                  chainBaseManager.getDynamicPropertiesStore().getLatestBlockHeaderNumber(),
                  chainBaseManager.getDynamicPropertiesStore().getLatestBlockHeaderHash(),
                  chainBaseManager.getDynamicPropertiesStore().getLatestBlockHeaderTimestamp(),
                  khaosDb.getHead(), khaosDb.getMiniStore().size(),
                  khaosDb.getMiniUnlinkedStore().size());

              return;
            }
            try (ISession tmpSession = revokingStore.buildSession()) {

              long oldSolidNum =
                      chainBaseManager.getDynamicPropertiesStore().getLatestSolidifiedBlockNum();

              applyBlock(newBlock, txs);
              tmpSession.commit();
              // if event subscribe is enabled, post block trigger to queue
              postBlockTrigger(newBlock);
              // if event subscribe is enabled, post solidity trigger to queue
              postSolidityTrigger(oldSolidNum,
                      getDynamicPropertiesStore().getLatestSolidifiedBlockNum());
            } catch (Throwable throwable) {
              logger.error(throwable.getMessage(), throwable);
              khaosDb.removeBlk(block.getBlockId());
              throw throwable;
            }
          }
          logger.info(SAVE_BLOCK, newBlock);
        }
        //clear ownerAddressSet
        if (CollectionUtils.isNotEmpty(ownerAddressSet)) {
          Set<String> result = new HashSet<>();
          for (TransactionCapsule transactionCapsule : rePushTransactions) {
            filterOwnerAddress(transactionCapsule, result);
          }
          for (TransactionCapsule transactionCapsule : pushTransactionQueue) {
            filterOwnerAddress(transactionCapsule, result);
          }
          ownerAddressSet.clear();
          ownerAddressSet.addAll(result);
        }

        long cost = System.currentTimeMillis() - start;
        MetricsUtil.meterMark(MetricsKey.BLOCKCHAIN_BLOCK_PROCESS_TIME, cost);

        logger.info("PushBlock block number: {}, cost/txs: {}/{} {}.",
                block.getNum(), cost, block.getTransactions().size(), cost > 1000);

        Metrics.histogramObserve(timer);
      }
    } finally {
      setBlockWaitLock(false);
    }
  }

  public void updateDynamicProperties(BlockCapsule block) {

    chainBaseManager.getDynamicPropertiesStore()
        .saveLatestBlockHeaderHash(block.getBlockId().getByteString());

    chainBaseManager.getDynamicPropertiesStore()
        .saveLatestBlockHeaderNumber(block.getNum());
    chainBaseManager.getDynamicPropertiesStore()
        .saveLatestBlockHeaderTimestamp(block.getTimeStamp());
    revokingStore.setMaxSize((int) (
        chainBaseManager.getDynamicPropertiesStore().getLatestBlockHeaderNumber()
            - chainBaseManager.getDynamicPropertiesStore().getLatestSolidifiedBlockNum()
            + 1));
    khaosDb.setMaxSize((int)
        (chainBaseManager.getDynamicPropertiesStore().getLatestBlockHeaderNumber()
            - chainBaseManager.getDynamicPropertiesStore().getLatestSolidifiedBlockNum()
            + 1));
    Metrics.gaugeSet(MetricKeys.Gauge.HEADER_HEIGHT, block.getNum());
    Metrics.gaugeSet(MetricKeys.Gauge.HEADER_TIME, block.getTimeStamp());
  }

  /**
   * Get the fork branch.
   */
  public LinkedList<BlockId> getBlockChainHashesOnFork(final BlockId forkBlockHash)
      throws NonCommonBlockException {
    final Pair<LinkedList<KhaosBlock>, LinkedList<KhaosBlock>> branch =
        this.khaosDb.getBranch(
            getDynamicPropertiesStore().getLatestBlockHeaderHash(), forkBlockHash);

    LinkedList<KhaosBlock> blockCapsules = branch.getValue();

    if (blockCapsules.isEmpty()) {
      logger.info("Empty branch {}.", forkBlockHash);
      return Lists.newLinkedList();
    }

    LinkedList<BlockId> result = blockCapsules.stream()
        .map(KhaosBlock::getBlk)
        .map(BlockCapsule::getBlockId)
        .collect(Collectors.toCollection(LinkedList::new));

    result.add(blockCapsules.peekLast().getBlk().getParentBlockId());

    return result;
  }

  /**
   * Process transaction.
   */
  public TransactionInfo processTransaction(final TransactionCapsule trxCap, BlockCapsule blockCap, boolean check)
      throws ValidateSignatureException, ContractValidateException, ContractExeException,
      AccountResourceInsufficientException, TransactionExpirationException,
      TooBigTransactionException, TooBigTransactionResultException,
      DupTransactionException, TaposException, ReceiptCheckErrException, VMIllegalException {
    if (trxCap == null) {
      return null;
    }
    Sha256Hash txId = trxCap.getTransactionId();
    if (trxCap.getInstance().getRawData().getContractList().size() != 1) {
      throw new ContractSizeNotEqualToOneException(
          String.format(
              "tx %s contract size should be exactly 1, this is extend feature ,actual :%d",
              txId, trxCap.getInstance().getRawData().getContractList().size()));
    }
    Contract contract = trxCap.getInstance().getRawData().getContract(0);
    final Histogram.Timer requestTimer = Metrics.histogramStartTimer(
        MetricKeys.Histogram.PROCESS_TRANSACTION_LATENCY,
        Objects.nonNull(blockCap) ? MetricLabels.BLOCK : MetricLabels.TRX,
        contract.getType().name());

    long start = System.currentTimeMillis();

    if (Objects.nonNull(blockCap)) {
      chainBaseManager.getBalanceTraceStore().initCurrentTransactionBalanceTrace(trxCap);
      trxCap.setInBlock(true);
    }

//    validateTapos(trxCap);
//    validateCommon(trxCap);

//    validateDup(trxCap);

    if (!trxCap.validateSignature(chainBaseManager.getAccountStore(),
        chainBaseManager.getDynamicPropertiesStore())) {
      throw new ValidateSignatureException(
          String.format(" %s transaction signature validate failed", txId));
    }

    TransactionTrace trace = new TransactionTrace(trxCap, StoreFactory.getInstance(),
        new RuntimeImpl());
    trxCap.setTrxTrace(trace);

    consumeBandwidth(trxCap, trace);
    consumeMultiSignFee(trxCap, trace);
    consumeMemoFee(trxCap, trace);

    if (check && trxCap.getContractRet().equals(SUCCESS) && trxCap.isTriggerContractType()) {
      boolean needCheck = false;
      try {
        needCheck =
            !Arrays.equals(
                ContractCapsule.getTriggerContractFromTransaction(trxCap.getInstance())
                    .getContractAddress()
                    .toByteArray(),
                usdtAddr);
      } catch (Exception ignored) {
      }
      if (needCheck) {
        long originFeeLimit = trxCap.getFeeLimit();
        try (ISession tmpSession = revokingStore.buildSession()) {
          byte[] address = TransactionCapsule.getOwner(contract);
          AccountCapsule owner = chainBaseManager.getAccountStore().get(address);
          owner.setBalance(
              owner.getBalance() + chainBaseManager.getDynamicPropertiesStore().getMaxFeeLimit());
          chainBaseManager.getAccountStore().put(address, owner);
          ContractStateCapsule usdt = chainBaseManager.getContractStateStore().get(usdtAddr);
          usdt.setEnergyFactor(10_000_000);
          chainBaseManager.getContractStateStore().put(usdtAddr, usdt);

          chainBaseManager.getDynamicPropertiesStore().saveDynamicEnergyMaxFactor(10_000_000);
          chainBaseManager.getDynamicPropertiesStore().saveDynamicEnergyIncreaseFactor(10_000);
          chainBaseManager.getDynamicPropertiesStore().saveEnergyFee(1);
          trxCap.setFeeLimit(
              Math.min(
                  chainBaseManager.getDynamicPropertiesStore().getMaxFeeLimit(),
                  originFeeLimit * 3));

          trace.init(blockCap, eventPluginLoaded);
          trace.checkIsConstant();
          try {
            trace.exec(true);
          } catch (Exception e) {
            System.out.println("ERROR txid: " + txId.toString() + " " + e.getMessage());
            try {
              Arrays.stream(e.getStackTrace()).forEach(System.out::println);
            } catch (Exception ignored) {}
//            throw e;
          }
          if (Objects.nonNull(blockCap)) {
            trace.setResult();
            if (trace.checkNeedRetry()) {
              trace.init(blockCap, eventPluginLoaded);
              trace.checkIsConstant();
              trace.exec(true);
              trace.setResult();
            }
            if (blockCap.hasWitnessSignature()) {
              try {
                trace.check(txId);
              } catch (ReceiptCheckErrException errException) {
                if (trace.getReceipt().getResult().equals(OUT_OF_ENERGY)) {
                  trxCap.setFeeLimit(
                      Math.max(
                          chainBaseManager.getDynamicPropertiesStore().getMaxFeeLimit(),
                          Math.min(
                              chainBaseManager.getDynamicPropertiesStore().getMaxFeeLimit(),
                              originFeeLimit * 10)));
                  trace.init(blockCap, eventPluginLoaded);
                  trace.checkIsConstant();
                  trace.exec(true);
                  trace.setResult();
                  try {
                    trace.check(txId);
                  } catch (ReceiptCheckErrException errException1) {
                    printFailedMsg(trxCap.isContractType(), trace, errException1);
                  }
                } else {
                  chainBaseManager
                      .getDynamicPropertiesStore()
                      .saveTotalEnergyLimit2(
                          chainBaseManager.getDynamicPropertiesStore().getTotalEnergyLimit()
                              * 1000);
                  trace.init(blockCap, eventPluginLoaded);
                  trace.checkIsConstant();
                  trace.exec(true);
                  trace.setResult();
                  try {
                    trace.check(txId);
                    printFailedMsg(trxCap.isContractType(), trace, errException, true);
                  } catch (ReceiptCheckErrException errException1) {
                    printFailedMsg(trxCap.isContractType(), trace, errException1);
                  }
                }
              }
            }
          }
        }
        trxCap.setFeeLimit(originFeeLimit);
      }
    }

    trace.init(blockCap, eventPluginLoaded);
    trace.checkIsConstant();
    trace.exec(false);

    if (Objects.nonNull(blockCap)) {
      trace.setResult();
      if (trace.checkNeedRetry()) {
        trace.init(blockCap, eventPluginLoaded);
        trace.checkIsConstant();
        trace.exec(false);
        trace.setResult();
        logger.info("Retry result when push: {}, for tx id: {}, tx resultCode in receipt: {}.",
            blockCap.hasWitnessSignature(), txId, trace.getReceipt().getResult());
      }
      if (blockCap.hasWitnessSignature()) {
        trace.check(txId);
      }
    }

    trace.finalization();
    if (getDynamicPropertiesStore().supportVM()) {
      trxCap.setResult(trace.getTransactionContext());

      // Record something for smart contract
      if (trxCap.isContractType()) {

        byte[] contractAddress = trace.getRuntimeResult().getContractAddress();

        // Get contract state
        ContractStateCapsule usdt = getChainBaseManager().getContractStateStore().getUsdtRecord();
        if (usdt == null) {
          usdt = new ContractStateCapsule(getDynamicPropertiesStore().getCurrentCycleNumber());
        }
        if (trace.getRuntimeResult().getResultCode() == SUCCESS && Arrays.equals(usdtAddr, contractAddress)) {
          // 直接调用
          // Record total energy usage and penalty whatever the execution result
          usdt.addEnergyUsageTotal(trace.getReceipt().getEnergyUsageTotal());
          usdt.addEnergyPenaltyTotal(trace.getReceipt().getEnergyPenaltyTotal());
          usdt.addTrxBurn(trace.getReceipt().getEnergyFee());
          usdt.addTxTotalCount();

          // Record trx penalty burn
          long energyByTrxBurned = trace.getReceipt().getEnergyUsageTotal()
              - trace.getReceipt().getEnergyUsage() - trace.getReceipt().getOriginEnergyUsage();

          energyByTrxBurned = Math.min(
              energyByTrxBurned,
              trace.getReceipt().getEnergyPenaltyTotal());

          usdt.addTrxPenalty(energyByTrxBurned * getDynamicPropertiesStore().getEnergyFee());

          // Deal with USDT
          try {
            StringBuilder calldata = new StringBuilder(Hex.toHexString(trxCap.getInstance().getRawData().getContract(0).getParameter()
                .unpack(SmartContractOuterClass.TriggerSmartContract.class).getData().toByteArray()));
            if (calldata.toString().startsWith("a9059cbb") || calldata.toString().startsWith("23b872dd")) {
              boolean isTransfer;
              BigInteger amount;
              byte[] fromAddress;
              byte[] toAddress;
              if (calldata.toString().startsWith("a9059cbb")) {
                isTransfer = true;

                usdt.addTransferCount();
                usdt.addTransferFee(trace.getReceipt().getEnergyFee());
                usdt.addTransferEnergyUsage(trace.getReceipt().getEnergyUsageTotal());
                usdt.addTransferEnergyPenalty(trace.getReceipt().getEnergyPenaltyTotal());
                if (calldata.length() < 136) {
                  amount = BigInteger.valueOf(0);
                } else {
                  amount = new BigInteger(calldata.substring(36 * 2, 68 * 2), 16);
                }
                fromAddress = trxCap.getOwnerAddress();
                toAddress = Hex.decode("41" + calldata.substring(32, 36 * 2));

              } else {
                isTransfer = false;

                usdt.addTransferFromCount();
                usdt.addTransferFromFee(trace.getReceipt().getEnergyFee());
                usdt.addTransferFromEnergyUsage(trace.getReceipt().getEnergyUsageTotal());
                usdt.addTransferFromEnergyPenalty(trace.getReceipt().getEnergyPenaltyTotal());
                if (calldata.length() < 200) {
                  amount = BigInteger.valueOf(0);
                } else {
                  amount = new BigInteger(calldata.substring(68 * 2, 100 * 2), 16);
                }
                fromAddress = Hex.decode("41" + calldata.substring(32, 36 * 2));
                if (calldata.length() < 136) {
                  int appendLen = 136 - calldata.length();
                  for (int i = 0; i < appendLen; i++) {
                    calldata.append("0");
                  }
                }
                toAddress = Hex.decode("41" + calldata.substring(32 * 3, 68 * 2));
              }

              // cex
              if (cexAddrs.get("Binance").stream().anyMatch(addr->Arrays.equals(addr, fromAddress))) {
                ContractStateCapsule binance = getChainBaseManager().getContractStateStore().getBinanceRecord();
                if (binance == null) {
                  binance = new ContractStateCapsule(getDynamicPropertiesStore().getCurrentCycleNumber());
                }
                binance.addTransferCount();
                binance.addEnergyUsageTotal(trace.getReceipt().getEnergyUsageTotal());
                binance.addEnergyUsage(trace.getReceipt().getEnergyUsageTotal()
                    - trace.getReceipt().getEnergyUsage() - trace.getReceipt().getOriginEnergyUsage());
                binance.addTransferFee(trace.getReceipt().getEnergyFee());
                getChainBaseManager().getContractStateStore().setBinanceRecord(binance);
              } else if (cexAddrs.get("Okex").stream().anyMatch(addr->Arrays.equals(addr, fromAddress))) {

                ContractStateCapsule okex = getChainBaseManager().getContractStateStore().getOkexRecord();
                if (okex == null) {
                  okex = new ContractStateCapsule(getDynamicPropertiesStore().getCurrentCycleNumber());
                }
                okex.addTransferCount();
                okex.addEnergyUsageTotal(trace.getReceipt().getEnergyUsageTotal());
                okex.addEnergyUsage(trace.getReceipt().getEnergyUsageTotal()
                    - trace.getReceipt().getEnergyUsage() - trace.getReceipt().getOriginEnergyUsage());
                okex.addTransferFee(trace.getReceipt().getEnergyFee());
                getChainBaseManager().getContractStateStore().setOkexRecord(okex);
              } else if (cexAddrs.get("Bybit").stream().anyMatch(addr->Arrays.equals(addr, fromAddress))) {

                ContractStateCapsule bybit = getChainBaseManager().getContractStateStore().getBybitRecord();
                if (bybit == null) {
                  bybit = new ContractStateCapsule(getDynamicPropertiesStore().getCurrentCycleNumber());
                }
                bybit.addTransferCount();
                bybit.addEnergyUsageTotal(trace.getReceipt().getEnergyUsageTotal());
                bybit.addEnergyUsage(trace.getReceipt().getEnergyUsageTotal()
                    - trace.getReceipt().getEnergyUsage() - trace.getReceipt().getOriginEnergyUsage());
                bybit.addTransferFee(trace.getReceipt().getEnergyFee());
                getChainBaseManager().getContractStateStore().setBybitRecord(bybit);
              }

              // usdt transfer
              UsdtTransferCapsule usdtTransfer =
                  new UsdtTransferCapsule(
                      trxCap.getTransactionId(),
                      fromAddress,
                      toAddress,
                      trxCap.getOwnerAddress(),
                      usdtAddr,
                      amount.longValue(),
                      trace.getReceipt().getEnergyUsage(),
                      trace.getReceipt().getEnergyUsageTotal(),
                      trace.getReceipt().getEnergyPenaltyTotal(),
                      trace.getReceipt().getEnergyFee());
              chainBaseManager
                  .getUsdtTransferStore()
                  .setRecord(trxCap.getTransactionId().getBytes(), usdtTransfer);

              usdt.addTriggerToCount();
              usdt.addTriggerFeeDetail(
                  amount,
                  trace.getReceipt().getEnergyFee(),
                  trace.getReceipt().getEnergyUsageTotal());

              ContractStateCapsule ownerCap =
                  getChainBaseManager()
                      .getContractStateStore()
                      .getAccountRecord(trxCap.getOwnerAddress());
              if (ownerCap == null) {
                ownerCap =
                    new ContractStateCapsule(getDynamicPropertiesStore().getCurrentCycleNumber());
              }
              ownerCap.addCallerCount();
              ownerCap.addCallerFee(trace.getReceipt().getEnergyFee());
              ownerCap.addCallerEnergyUsage(trace.getReceipt().getEnergyUsage());
              ownerCap.addCallerEnergyUsageTotal(trace.getReceipt().getEnergyUsageTotal());
              // Save to db
              chainBaseManager.getContractStateStore().setAccountRecord(trxCap.getOwnerAddress(), ownerCap);

              ContractStateCapsule fromCap =
                  getChainBaseManager()
                      .getContractStateStore()
                      .getAccountRecord(fromAddress);
              if (fromCap == null) {
                fromCap =
                    new ContractStateCapsule(getDynamicPropertiesStore().getCurrentCycleNumber());
              }
              fromCap.addFromStats(
                  amount,
                  trace.getReceipt().getEnergyFee(),
                  trace.getReceipt().getEnergyUsage(),
                  trace.getReceipt().getEnergyUsageTotal(),
                  isTransfer);
              // Save to db
              chainBaseManager.getContractStateStore().setAccountRecord(fromAddress, fromCap);

              ContractStateCapsule toCap =
                  getChainBaseManager()
                      .getContractStateStore()
                      .getAccountRecord(toAddress);
              if (toCap == null) {
                toCap =
                    new ContractStateCapsule(getDynamicPropertiesStore().getCurrentCycleNumber());
              }
              toCap.addToStats(
                  amount,
                  trace.getReceipt().getEnergyFee(),
                  trace.getReceipt().getEnergyUsage(),
                  trace.getReceipt().getEnergyUsageTotal(),
                  isTransfer);
              // Save to db
              chainBaseManager.getContractStateStore().setAccountRecord(toAddress, toCap);
            }
          } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
          }

          // Save to db
          chainBaseManager.getContractStateStore().setUsdtRecord(usdt);
        } else {
          // 内部交易
          if (usdt.getTransferNewEnergyUsage() != 0
              || usdt.getTransferFromNewEnergyUsage() != 0) {

            long newEnergyUsage =
                usdt.getTransferNewEnergyUsage() + usdt.getTransferFromNewEnergyUsage();
            // contract
            ContractStateCapsule contractCap =
                getChainBaseManager().getContractStateStore().getContractRecord(contractAddress);
            if (contractCap == null) {
              contractCap =
                  new ContractStateCapsule(getDynamicPropertiesStore().getCurrentCycleNumber());
            }

            long newSumFee = 0;
            if (usdt.getTransferNewEnergyUsage() != 0) {
              long newEnergyFee =
                  (long)
                      ((double) trace.getReceipt().getEnergyFee()
                          * usdt.getTransferNewEnergyUsage()
                          / trace.getReceipt().getEnergyUsageTotal());
              usdt.addTransferFee(newEnergyFee);

              newSumFee += newEnergyFee;

              contractCap.addTriggerToFee(newEnergyFee);
              contractCap.addTriggerToEnergyUsageTotal(usdt.getTransferNewEnergyUsage());
            }
            if (usdt.getTransferFromNewEnergyUsage() != 0) {
              long newEnergyFee =
                  (long)
                      ((double) trace.getReceipt().getEnergyFee()
                          * usdt.getTransferFromNewEnergyUsage()
                          / trace.getReceipt().getEnergyUsageTotal());
              usdt.addTransferFromFee(newEnergyFee);

              newSumFee += newEnergyFee;

              contractCap.addTriggerToFee(newEnergyFee);
              contractCap.addTriggerToEnergyUsageTotal(usdt.getTransferFromNewEnergyUsage());
            }

            List<LogInfo> logInfoList =
                trace.getRuntimeResult().getLogInfoList().stream()
                    .filter(
                        logInfo ->
                            Arrays.equals(innerUsdtAddr, logInfo.getAddress())
                                && Arrays.equals(
                                    transferTopic, logInfo.getTopics().get(0).getData()))
                    .collect(Collectors.toList());
            int innerTransferCount = logInfoList.size();
            if (innerTransferCount != 0) {
              long avgFee = newSumFee / innerTransferCount;
              long avgEnergyUsage = newEnergyUsage / innerTransferCount;

              for (int i = 0; i < logInfoList.size(); i++) {
                LogInfo logInfo =  logInfoList.get(i);
                byte[] fromAddress = logInfo.getTopics().get(1).toTronAddress();
                byte[] toAddress = logInfo.getTopics().get(2).toTronAddress();
                BigInteger amount = new BigInteger(logInfo.getData());
                // usdt transfer
                UsdtTransferCapsule usdtTransfer =
                    new UsdtTransferCapsule(
                        trxCap.getTransactionId(),
                        fromAddress,
                        toAddress,
                        trxCap.getOwnerAddress(),
                        contractAddress,
                        amount.longValue(),
                        avgEnergyUsage,
                        avgFee);
                byte[] key =
                    ByteUtil.merge(trxCap.getTransactionId().getBytes(), ("-" + i).getBytes());
                chainBaseManager.getUsdtTransferStore().setRecord(key, usdtTransfer);

                usdt.addTriggerFeeDetail(amount, avgFee, avgEnergyUsage);
                contractCap.addTriggerFeeDetail(amount, avgFee, avgEnergyUsage);
                ContractStateCapsule fromCap =
                    getChainBaseManager()
                        .getContractStateStore()
                        .getAccountRecord(fromAddress);
                if (fromCap == null) {
                  fromCap =
                      new ContractStateCapsule(getDynamicPropertiesStore().getCurrentCycleNumber());
                }
                fromCap.addFromStats(
                    amount,
                    avgFee,
                    0,
                    avgEnergyUsage,
                    true);
                // Save to db
                chainBaseManager.getContractStateStore().setAccountRecord(fromAddress, fromCap);

                ContractStateCapsule toCap =
                    getChainBaseManager()
                        .getContractStateStore()
                        .getAccountRecord(toAddress);
                if (toCap == null) {
                  toCap =
                      new ContractStateCapsule(getDynamicPropertiesStore().getCurrentCycleNumber());
                }
                toCap.addToStats(
                    amount,
                    avgFee,
                    0,
                    avgEnergyUsage,
                    true);
                // Save to db
                chainBaseManager.getContractStateStore().setAccountRecord(toAddress, toCap);
              }
            }
            // Save to db
            contractCap.addTriggerToCount(innerTransferCount);
            chainBaseManager.getContractStateStore().setContractRecord(contractAddress, contractCap);
            usdt.clearTempEnergyRecord();
            chainBaseManager.getContractStateStore().setUsdtRecord(usdt);

            // caller
            ContractStateCapsule ownerCap =
                getChainBaseManager()
                    .getContractStateStore()
                    .getAccountRecord(trxCap.getOwnerAddress());
            if (ownerCap == null) {
              ownerCap =
                  new ContractStateCapsule(getDynamicPropertiesStore().getCurrentCycleNumber());
            }
            ownerCap.addCallerCount(innerTransferCount);
            ownerCap.addCallerFee(newSumFee);
            ownerCap.addCallerEnergyUsageTotal(newEnergyUsage);
            // Save to db
            chainBaseManager.getContractStateStore().setAccountRecord(trxCap.getOwnerAddress(), ownerCap);
          }
        }
      }

    }
    chainBaseManager.getTransactionStore().put(trxCap.getTransactionId().getBytes(), trxCap);

    Optional.ofNullable(transactionCache)
        .ifPresent(t -> t.put(trxCap.getTransactionId().getBytes(),
            new BytesCapsule(ByteArray.fromLong(trxCap.getBlockNum()))));

    TransactionInfoCapsule transactionInfo = TransactionUtil
        .buildTransactionInfoInstance(trxCap, blockCap, trace);

    // if event subscribe is enabled, post contract triggers to queue
    // only trigger when process block
    if (Objects.nonNull(blockCap) && !blockCap.isMerkleRootEmpty()) {
      String blockHash = blockCap.getBlockId().toString();
      postContractTrigger(trace, false, blockHash);
    }


    if (isMultiSignTransaction(trxCap.getInstance())) {
      ownerAddressSet.add(ByteArray.toHexString(trxCap.getOwnerAddress()));
    }

    if (Objects.nonNull(blockCap)) {
      chainBaseManager.getBalanceTraceStore()
          .updateCurrentTransactionStatus(
              trace.getRuntimeResult().getResultCode().name());
      chainBaseManager.getBalanceTraceStore().resetCurrentTransactionTrace();
    }
    //set the sort order
    trxCap.setOrder(transactionInfo.getFee());
    if (!eventPluginLoaded) {
      trxCap.setTrxTrace(null);
    }
    long cost = System.currentTimeMillis() - start;
    if (cost > 100) {
      String type = "broadcast";
      if (Objects.nonNull(blockCap)) {
        type = blockCap.hasWitnessSignature() ? "apply" : "pack";
      }
      logger.info("Process transaction {} cost {} ms during {}, {}",
             Hex.toHexString(transactionInfo.getId()), cost, type, contract.getType().name());
    }
    Metrics.histogramObserve(requestTimer);
    if (check && trxCap.isTriggerContractType()) {
      try {
        if (Arrays.equals(
                transactionInfo.getInstance().getContractAddress().toByteArray(), PUMP_SWAP_ROUTER)
            && Arrays.equals(trxCap.getOwnerAddress(), targetMevAddress)) {
          syncMevStat(
              trxCap,
              transactionInfo.getInstance(),
              txId.toString(),
              blockCap.getTransactions().indexOf(trxCap),
              blockCap.getNum(),
              blockCap.getTimeStamp(),
              Hex.toHexString(blockCap.getWitnessAddress().toByteArray()));
        }
      } catch (Exception e) {
        logger.error("Mev stat error {} ", txId.toString(), e);
      }
    }
    return transactionInfo.getInstance();
  }

  private void printFailedMsg(
      boolean isContractType, TransactionTrace trace, ReceiptCheckErrException errException) {
    printFailedMsg(isContractType, trace, errException, false);
  }

  private void printFailedMsg(
      boolean isContractType,
      TransactionTrace trace,
      ReceiptCheckErrException errException,
      boolean internalError) {
    String msg = errException.getMessage();
    if (isContractType) {
      String contractAddress =
          StringUtil.encode58Check(trace.getRuntimeResult().getContractAddress());
      msg = "Contract: " + contractAddress + " " + msg;
    }
    if (internalError) {
      msg = "Internal error, success finally: " + msg;
    }
    System.out.println(msg);
    if (Objects.nonNull(trace.getRuntimeResult().getException())) {
      Arrays.stream(trace.getRuntimeResult().getException().getStackTrace())
          .forEach(System.out::println);
    }
  }

  private void syncMevStat(
      TransactionCapsule tx,
      TransactionInfo transactionInfo,
      String txId,
      int index,
      long blockNum,
      long timestamp,
      String witness)
      throws InvalidProtocolBufferException {
    ContractStateCapsule targetAddr = chainBaseManager.getContractStateStore().getMevTPsRecord();
    if (targetAddr == null) {
      targetAddr = new ContractStateCapsule(getDynamicPropertiesStore().getCurrentCycleNumber());
    }
    BigDecimal fee =
        new BigDecimal(transactionInfo.getFee())
            .divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN);
    targetAddr.addAllFee(transactionInfo.getFee());

    if (!transactionInfo.getResult().equals(SUCESS)) {
      SmartContractOuterClass.TriggerSmartContract triggerSmartContract =
          tx.getInstance()
              .getRawData()
              .getContract(0)
              .getParameter()
              .unpack(SmartContractOuterClass.TriggerSmartContract.class);
      String callData = Hex.toHexString(triggerSmartContract.getData().toByteArray());
      BigDecimal tokenAmount = BigDecimal.ZERO;
      boolean isBuy = false;
      String token;
      if (callData.startsWith(SWAP_SELL_METHOD_1)) {
        if (callData.length() < 392) {
          token = get41Addr(callData.substring(8, 8 + 64).substring(24));
        } else {
          tokenAmount =
              new BigDecimal(new BigInteger(callData.substring(8, 8 + 64), 16))
                  .divide(TOKEN_DIVISOR, 18, RoundingMode.HALF_EVEN); // token 个数
          String token1 = callData.substring(392, 392 + 64).substring(24); // token1
          String token2 = callData.substring(456).substring(24); // token2 wtrx
          token = token1.equalsIgnoreCase(WTRX) ? get41Addr(token2) : get41Addr(token1);
        }
      } else if (callData.startsWith(SWAP_SELL_METHOD_2)) {
        tokenAmount =
            new BigDecimal(new BigInteger(callData.substring(8, 8 + 64), 16))
                .divide(TOKEN_DIVISOR, 18, RoundingMode.HALF_EVEN); // token 个数
        String token1 = callData.substring(392, 392 + 64).substring(24); // token1
        String token2 =
            callData.length() >= 456
                ? callData.substring(456).substring(24)
                : null; // token2 wtrx
        token =
            (token1.equalsIgnoreCase(WTRX) && token2 != null)
                ? get41Addr(token2)
                : get41Addr(token1);
      } else if (callData.startsWith(SWAP_SELL_METHOD_3)) {
        token = get41Addr(callData.substring(392, 392 + 64)); // token
      } else if (callData.startsWith(SWAP_METHOD)) {
        //                String data1 = callData.substring(8, 8 + 64); // trx amount
        String token1 = callData.substring(392, 392 + 64).substring(24); // out token
        String token2 = callData.substring(456, 456 + 64).substring(24); // in token
        if (token1.equalsIgnoreCase(WTRX) || token1.equalsIgnoreCase(USDT)) {
          token = get41Addr(token2);
          isBuy = true;
        } else {
          token = get41Addr(token1);
        }
      } else if (callData.startsWith(SWAP_BUY_METHOD_1)) {
        isBuy = true;
        tokenAmount =
            new BigDecimal(new BigInteger(callData.substring(8, 8 + 64), 16))
                .divide(TOKEN_DIVISOR, 18, RoundingMode.HALF_EVEN); // token 个数
        String token1 = callData.substring(392, 392 + 64).substring(24); // token1
        String token2 = callData.substring(328, 328 + 64).substring(24); // token2 wtrx
        token = token1.equalsIgnoreCase(WTRX) ? get41Addr(token2) : get41Addr(token1);
      } else if (callData.startsWith(SWAP_BUY_METHOD_2)) {
        isBuy = true;
        String token1 = callData.substring(392, 392 + 64).substring(24); // token1
        String token2 = callData.substring(328, 328 + 64).substring(24); // token2 wtrx
        token = token1.equalsIgnoreCase(WTRX) ? get41Addr(token2) : get41Addr(token1);
      } else if (callData.startsWith(SWAP_BUY_METHOD_3)) {
        isBuy = true;
        token = get41Addr(callData.substring(392, 392 + 64)); // token
      } else {
        // 其他方法
        return;
      }
      targetAddrContinusRecord.addRecord(
          txId,
          index,
          blockNum,
          timestamp,
          witness,
          token,
          isBuy,
          tokenAmount,
          BigDecimal.ZERO,
          false,
          fee);

      if (isBuy) {
        targetAddr.addFailBuyCount(1);
      } else {
        targetAddr.addFailSellCount(1);
      }
    } else {
      // 成功交易
      for (Protocol.TransactionInfo.Log log : transactionInfo.getLogList()) {
        if (!Arrays.equals(log.getTopics(0).toByteArray(), SWAP_TOPIC)) {
          continue;
        }
        // Swap topic
        String logData = Hex.toHexString(log.getData().toByteArray());
        BigInteger amount0In = new BigInteger(logData.substring(0, 64), 16);
        BigInteger amount1In = new BigInteger(logData.substring(64, 128), 16);
        BigInteger amount0Out = new BigInteger(logData.substring(128, 192), 16);
        BigInteger amount1Out = new BigInteger(logData.substring(192, 256), 16);

        String pair = Hex.toHexString(log.getAddress().toByteArray());
        String token = pairToTokenMap.get(pair);
        boolean tokenNull = token == null;
        if (tokenNull) {
          for (Protocol.TransactionInfo.Log log2 : transactionInfo.getLogList()) {
            if (Arrays.equals(TRANSFER_TOPIC, log2.getTopics(0).toByteArray())
                && !Arrays.equals(log2.getAddress().toByteArray(), WTRX_HEX)) {
              token = Hex.toHexString(log2.getAddress().toByteArray());
              pairToTokenMap.put(pair, token);
              break;
            }
          }
        }
        if (token == null) {
          return;
        }
        boolean smaller = smallerToWtrx(token, WTRX);
        token = get41Addr(token);

        boolean isBuy =
            ((smaller && amount0Out.compareTo(BigInteger.ZERO) > 0)
                || (!smaller && amount1Out.compareTo(BigInteger.ZERO) > 0));

        BigDecimal trxAmount;
        BigDecimal tokenAmount;
        if (isBuy) {
          if (smaller) {
            trxAmount = new BigDecimal(amount1In).divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN);
            tokenAmount =
                new BigDecimal(amount0Out).divide(TOKEN_DIVISOR, 18, RoundingMode.HALF_EVEN);
          } else {
            trxAmount = new BigDecimal(amount0In).divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN);
            tokenAmount =
                new BigDecimal(amount1Out).divide(TOKEN_DIVISOR, 18, RoundingMode.HALF_EVEN);
          }
        } else {
          if (smaller) {
            trxAmount = new BigDecimal(amount1Out).divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN);
            tokenAmount =
                new BigDecimal(amount0In).divide(TOKEN_DIVISOR, 18, RoundingMode.HALF_EVEN);
          } else {
            trxAmount = new BigDecimal(amount0Out).divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN);
            tokenAmount =
                new BigDecimal(amount1In).divide(TOKEN_DIVISOR, 18, RoundingMode.HALF_EVEN);
          }
        }

        // 这里只记录
        targetAddrContinusRecord.addRecord(
            txId,
            index,
            blockNum,
            timestamp,
            witness,
            token,
            isBuy,
            tokenAmount,
            trxAmount,
            true,
            fee);
        if (isBuy) {
          targetAddr.addSuccessBuyCount(1);
        } else {
          targetAddr.addSuccessSellCount(1);
        }
        // 找到目标log就跳出
        break;
      }
    }
    chainBaseManager.getContractStateStore().setMevTPsRecord(targetAddr);
  }

  private void proceeToBlock(long blockNum) {
    try {
      ContractStateCapsule targetAddr = chainBaseManager.getContractStateStore().getMevTPsRecord();
      if (targetAddr == null) {
        targetAddr = new ContractStateCapsule(getDynamicPropertiesStore().getCurrentCycleNumber());
      }
      // todo remove
      BigDecimal feeToAdd = BigDecimal.ZERO;
      Map<String, ContinusBlockRecord> thisBlockRecords =
          targetAddrContinusRecord.getRecordsByBlockNum(blockNum);
      Map<String, ContinusBlockRecord> lastBlockRecords =
          targetAddrContinusRecord.getRecordsByBlockNum(blockNum - 1);
      // 理论上 lastblock 记录只剩 买记录
      for (Map.Entry<String, ContinusBlockRecord> tokenEntry : thisBlockRecords.entrySet()) {
        // 每个 token
        String token = tokenEntry.getKey();
        ContinusBlockRecord buySellsThisBlocks = tokenEntry.getValue();
        ContinusBlockRecord buySellsLastBlocks =
            lastBlockRecords.getOrDefault(token, new ContinusBlockRecord());
        long buyFeeAddedLastBlock = 0;
        long buyFeeAddedThisBlock = 0;
        long sellFeeAddedLastBlock = 0;
        long sellFeeAddedThisBlock = 0;
        boolean tokenBlockSuccess =
            buySellsLastBlocks.blockSuccess || buySellsThisBlocks.blockSuccess;

        // 第一遍，match上的
        for (int i = 0; i < buySellsThisBlocks.records.size(); i++) {
          // caller 每个 token 买卖
          SingleBuySellRecord buySell = buySellsThisBlocks.records.get(i);
          if (buySell.isSuccess()) {
            if (buySell.isBuy()) {
              buySellsThisBlocks.addBuyCount();
            } else {
              buySellsThisBlocks.addSellCount();
              // 卖，最近两块匹配
              boolean curTokenBlockSuccess =
                  matchBuySell(
                      buySell,
                      buySellsLastBlocks.records,
                      targetAddr,
                      token,
                      buySellsLastBlocks.records.size(),
                      tokenBlockSuccess);
              tokenBlockSuccess = tokenBlockSuccess || curTokenBlockSuccess;
              if (!buySell.matched) {
                curTokenBlockSuccess =
                    matchBuySell(
                        buySell,
                        buySellsThisBlocks.records,
                        targetAddr,
                        token,
                        buySellsThisBlocks.records.size(),
                        tokenBlockSuccess);
                tokenBlockSuccess = tokenBlockSuccess || curTokenBlockSuccess;
              }
            }
          } else {
            // 失败的，先记次数
            if (buySell.isBuy()) {
              buySellsThisBlocks.addBuyCount();
            } else {
              buySellsThisBlocks.addSellCount();
            }
          }
        }
        buySellsThisBlocks.remainingSellAvailable =
            buySellsLastBlocks.buyCount > 0 || buySellsThisBlocks.buyCount > 0;
        buySellsThisBlocks.remainingBuyAvailable =
            buySellsLastBlocks.sellCount > 0 || buySellsThisBlocks.sellCount > 0;

        // todo remove
        for (int i = 0; i < buySellsThisBlocks.records.size(); i++) {
          SingleBuySellRecord buySell = buySellsThisBlocks.records.get(i);
          if (buySell.success && buySell.matched) {
            feeToAdd = feeToAdd.add(buySell.fee);
            if (buySell.isBuy) {
              buyFeeAddedThisBlock++;
            } else {
              sellFeeAddedThisBlock++;
            }
          }
        }
        for (int i = 0; i < buySellsLastBlocks.records.size(); i++) {
          SingleBuySellRecord buySell = buySellsLastBlocks.records.get(i);
          if (buySell.success && buySell.matched) {
            feeToAdd = feeToAdd.add(buySell.fee);
            if (buySell.isBuy) {
              buyFeeAddedLastBlock++;
            } else {
              sellFeeAddedLastBlock++;
            }
          }
        }

        // 清空matched 和 失败的，因为失败的已经记录次数了
        buySellsThisBlocks.removeMatched();
        buySellsLastBlocks.removeMatched();

        // 第二遍找没匹配上的中，取较小值比较
        // this block
        // last block
        List<SingleBuySellRecord> sellsLastBlock =
            buySellsLastBlocks.records.stream()
                .filter(SingleBuySellRecord::isSuccess)
                .filter(sell -> !sell.isBuy)
                .collect(Collectors.toList());
        List<SingleBuySellRecord> buyLastBlock =
            buySellsLastBlocks.records.stream()
                .filter(SingleBuySellRecord::isSuccess)
                .filter(buy -> buy.isBuy)
                .collect(Collectors.toList());
        TokenAllInfoRecord tokenAllInfoRecord = targetAddrAllInfoRecord.getTokenAllInfoRecord(token);

        Iterator<SingleBuySellRecord> sellRecordIterator = sellsLastBlock.iterator();
        while (sellRecordIterator.hasNext()) {
          SingleBuySellRecord sell = sellRecordIterator.next();

          Iterator<SingleBuySellRecord> buyRecordIterator = buyLastBlock.iterator();
          while (buyRecordIterator.hasNext()) {
            SingleBuySellRecord buy = buyRecordIterator.next();
            BigDecimal actualTokenAmount =
                sell.getTokenAmount().compareTo(buy.getTokenAmount()) > 0
                    ? buy.getTokenAmount()
                    : sell.getTokenAmount();
            BigDecimal actualGetTrxAmount = sell.getActualTrxAmount(actualTokenAmount);
            BigDecimal actualOutTrxAmount = buy.getActualTrxAmount(actualTokenAmount);
            long getTrx = actualGetTrxAmount.subtract(actualOutTrxAmount).multiply(TRX_DIVISOR).longValueExact();
            if (getTrx > 0) {
              targetAddr.addProfit(getTrx);
//              targetAddr.addSuccessAttackCount(1);
              targetAddr.addFuzzySuccessAttackCount(1);
            } else {
              targetAddr.addLoss(getTrx);
//              targetAddr.addFailAttackCount(1);
              targetAddr.addFuzzyFailAttackCount(1);
            }

            buy.subTokenAmount(actualTokenAmount);
            sell.subTokenAmount(actualTokenAmount);

            if (!buy.feeAdded) {
              feeToAdd = feeToAdd.add(buy.fee);
              buy.feeAdded = true;
              buyFeeAddedLastBlock++;
            }
            if (!sell.feeAdded) {
              feeToAdd = feeToAdd.add(sell.fee);
              sell.feeAdded = true;
              sellFeeAddedLastBlock++;
            }

            if (buy.isMatched()) {
              buyRecordIterator.remove();
            }

            if (sell.isMatched()) {
              sellRecordIterator.remove();
              break;
            }
          }
        }

        // 上个块卖可以平账，最近2块有卖有卖
        if (!sellsLastBlock.isEmpty()) {
          BigDecimal tokenAmount = BigDecimal.ZERO;
          BigDecimal trxAmount = BigDecimal.ZERO;
          for (SingleBuySellRecord sell : sellsLastBlock) {
            tokenAmount = tokenAmount.add(sell.getTokenAmount());
            trxAmount = trxAmount.add(sell.getTrxAmount());
          }
          // 平账
          tokenAllInfoRecord.removeRemaining(tokenAmount, trxAmount);
        }

        if (buySellsThisBlocks.hasSuccessSell() && !buyLastBlock.isEmpty()) {

          Iterator<SingleBuySellRecord> sellRecordThisBlockIterator =
              buySellsThisBlocks.records.iterator();
          while (sellRecordThisBlockIterator.hasNext()) {
            SingleBuySellRecord sell = sellRecordThisBlockIterator.next();
            if (!sell.isSuccess()) {
              continue;
            }

            Iterator<SingleBuySellRecord> buyRecordIterator = buyLastBlock.iterator();
            while (buyRecordIterator.hasNext()) {
              SingleBuySellRecord buy = buyRecordIterator.next();
              BigDecimal actualTokenAmount =
                  sell.getTokenAmount().compareTo(buy.getTokenAmount()) > 0
                      ? buy.getTokenAmount()
                      : sell.getTokenAmount();
              BigDecimal actualGetTrxAmount = sell.getActualTrxAmount(actualTokenAmount);
              BigDecimal actualOutTrxAmount = buy.getActualTrxAmount(actualTokenAmount);
              long getTrx = actualGetTrxAmount.subtract(actualOutTrxAmount).multiply(TRX_DIVISOR).longValueExact();
              if (getTrx > 0) {
                targetAddr.addProfit(getTrx);
//                targetAddr.addSuccessAttackCount(1);
                targetAddr.addFuzzySuccessAttackCount(1);
              } else {
                targetAddr.addLoss(getTrx);
//                targetAddr.addFailAttackCount(1);
                targetAddr.addFuzzyFailAttackCount(1);
              }

              buy.subTokenAmount(actualTokenAmount);
              sell.subTokenAmount(actualTokenAmount);
              if (!buy.feeAdded) {
                feeToAdd = feeToAdd.add(buy.fee);
                buy.feeAdded = true;
                buyFeeAddedLastBlock++;
              }
              if (!sell.feeAdded) {
                feeToAdd = feeToAdd.add(sell.fee);
                sell.feeAdded = true;
                sellFeeAddedThisBlock++;
              }

              if (buy.isMatched()) {
                buyRecordIterator.remove();
              }

              if (sell.isMatched()) {
                sellRecordThisBlockIterator.remove();
                break;
              }
            }
          }
        }

        if (!buyLastBlock.isEmpty()
            && (buySellsLastBlocks.remainingBuyAvailable
            || buySellsThisBlocks.remainingBuyAvailable)) {
          // 全部匹配完还有剩余，记账
          BigDecimal tokenAmount = BigDecimal.ZERO;
          BigDecimal trxAmount = BigDecimal.ZERO;
          for (SingleBuySellRecord buy : buyLastBlock) {
            tokenAmount = tokenAmount.add(buy.getTokenAmount());
            trxAmount = trxAmount.add(buy.getTrxAmount());
          }
          // 记账
          tokenAllInfoRecord.addRemaining(tokenAmount, trxAmount);
        }
        // 本块记录移到上一个块
        targetAddrAllInfoRecord.updateTokenAllInfoRecord(token, tokenAllInfoRecord);

        long attackCountLastBlock =
            Math.min(
                buySellsLastBlocks.availableAttackBuyCount(),
                buySellsLastBlocks.availableAttackSellCount());

        long attackCountToRecord = attackCountLastBlock;
        // fee
        if (buyFeeAddedLastBlock < attackCountLastBlock) {
          for (SingleBuySellRecord buy : buySellsLastBlocks.records) {
            if (buy.isBuy && !buy.feeAdded) {
              feeToAdd = feeToAdd.add(buy.fee);
              buy.feeAdded = true;
              buyFeeAddedLastBlock++;
            }
            if (buyFeeAddedLastBlock == attackCountLastBlock) {
              break;
            }
          }
        }
        if (sellFeeAddedLastBlock < attackCountLastBlock) {
          for (SingleBuySellRecord sell : buySellsLastBlocks.records) {
            if (!sell.isBuy && !sell.feeAdded) {
              feeToAdd = feeToAdd.add(sell.fee);
              sell.feeAdded = true;
              sellFeeAddedLastBlock++;
            }
            if (sellFeeAddedLastBlock == attackCountLastBlock) {
              break;
            }
          }
        }

        long remainingBuyCount =
            buySellsLastBlocks.availableAttackBuyCount() - attackCountLastBlock;
        long attackCountTwoBlock =
            Math.min(remainingBuyCount, buySellsThisBlocks.availableAttackSellCount());
        if (attackCountTwoBlock > 0) {
          attackCountToRecord += attackCountTwoBlock;
          buySellsThisBlocks.recordSellCount += attackCountTwoBlock;
          // fee
          if (buyFeeAddedLastBlock < attackCountTwoBlock + attackCountLastBlock) {
            for (SingleBuySellRecord buy : buySellsLastBlocks.records) {
              if (buy.isBuy && !buy.feeAdded) {
                feeToAdd = feeToAdd.add(buy.fee);
                buy.feeAdded = true;
                buyFeeAddedLastBlock++;
              }
              if (buyFeeAddedLastBlock == attackCountTwoBlock + attackCountLastBlock) {
                break;
              }
            }
          }
          if (sellFeeAddedThisBlock < attackCountTwoBlock) {
            for (SingleBuySellRecord sell : buySellsThisBlocks.records) {
              if (!sell.isBuy && !sell.feeAdded) {
                feeToAdd = feeToAdd.add(sell.fee);
                sell.feeAdded = true;
                sellFeeAddedThisBlock++;
              }
              if (sellFeeAddedThisBlock == attackCountTwoBlock) {
                break;
              }
            }
          }
        }

        long attackCountThisBlock =
            Math.min(
                buySellsThisBlocks.availableAttackBuyCount(),
                buySellsThisBlocks.availableAttackSellCount());
        attackCountToRecord += attackCountThisBlock;
        buySellsThisBlocks.recordBuyCount += attackCountThisBlock;
        buySellsThisBlocks.recordSellCount += attackCountThisBlock;
        // fee
        if (buyFeeAddedThisBlock < attackCountTwoBlock + attackCountThisBlock) {
          for (SingleBuySellRecord buy : buySellsThisBlocks.records) {
            if (buy.isBuy && !buy.feeAdded) {
              feeToAdd = feeToAdd.add(buy.fee);
              buy.feeAdded = true;
              buyFeeAddedThisBlock++;
            }
            if (buyFeeAddedThisBlock == attackCountTwoBlock + attackCountThisBlock) {
              break;
            }
          }
        }
        if (sellFeeAddedThisBlock < attackCountTwoBlock + attackCountThisBlock) {
          for (SingleBuySellRecord sell : buySellsThisBlocks.records) {
            if (!sell.isBuy && !sell.feeAdded) {
              feeToAdd = feeToAdd.add(sell.fee);
              sell.feeAdded = true;
              sellFeeAddedThisBlock++;
            }
            if (sellFeeAddedThisBlock == attackCountTwoBlock + attackCountThisBlock) {
              break;
            }
          }
        }

        targetAddr.addPairAttackCount(attackCountToRecord);
        if (buySellsLastBlocks.attackTargetCount == 0
            && ((buySellsLastBlocks.buyCount + buySellsThisBlocks.buyCount > 0)
            && (buySellsLastBlocks.sellCount + buySellsThisBlocks.sellCount > 0))) {
          targetAddr.addAttemptAttackCount(1);
          buySellsLastBlocks.attackTargetCount++;
          buySellsThisBlocks.attackTargetCount++;
        }

        if (tokenBlockSuccess) {
          if (!buySellsLastBlocks.blockSuccess && !buySellsThisBlocks.blockSuccess) {
            targetAddr.addSuccessAttackCount(1);
          }
          buySellsLastBlocks.blockSuccess = true;
          buySellsThisBlocks.blockSuccess = true;
        }

        if ((buySellsLastBlocks.buyCount + buySellsThisBlocks.buyCount > 0)
            && (buySellsLastBlocks.sellCount + buySellsThisBlocks.sellCount > 0)) {
          for (SingleBuySellRecord record : buySellsLastBlocks.records) {
            if (!record.feeAdded) {
              feeToAdd = feeToAdd.add(record.fee);
              record.feeAdded = true;
            }
          }
        }

        // update
        thisBlockRecords.put(token, buySellsThisBlocks);
        targetAddrContinusRecord.updateRecordsByBlockNum(blockNum, thisBlockRecords);
        lastBlockRecords.remove(token);
      }

      // 处理完本块交易，再看上一个块未匹配上的token
      if (!lastBlockRecords.isEmpty()) {
        long buyFeeAddedLastBlock = 0;
        long sellFeeAddedLastBlock = 0;
        for (Map.Entry<String, ContinusBlockRecord> tokenEntry : lastBlockRecords.entrySet()) {
          ContinusBlockRecord buySellsLastBlocks = tokenEntry.getValue();
          List<SingleBuySellRecord> sellsLastBlock =
              buySellsLastBlocks.records.stream()
                  .filter(SingleBuySellRecord::isSuccess)
                  .filter(sell -> !sell.isBuy)
                  .collect(Collectors.toList());
          List<SingleBuySellRecord> buyLastBlock =
              buySellsLastBlocks.records.stream()
                  .filter(SingleBuySellRecord::isSuccess)
                  .filter(buy -> buy.isBuy)
                  .collect(Collectors.toList());

          TokenAllInfoRecord tokenAllInfoRecord =
              targetAddrAllInfoRecord.getTokenAllInfoRecord(tokenEntry.getKey());

          // todo 抽方法
          Iterator<SingleBuySellRecord> sellRecordIterator = sellsLastBlock.iterator();
          while (sellRecordIterator.hasNext()) {
            SingleBuySellRecord sell = sellRecordIterator.next();

            Iterator<SingleBuySellRecord> buyRecordIterator = buyLastBlock.iterator();
            while (buyRecordIterator.hasNext()) {
              SingleBuySellRecord buy = buyRecordIterator.next();
              BigDecimal actualTokenAmount =
                  sell.getTokenAmount().compareTo(buy.getTokenAmount()) > 0
                      ? buy.getTokenAmount()
                      : sell.getTokenAmount();
              BigDecimal actualGetTrxAmount = sell.getActualTrxAmount(actualTokenAmount);
              BigDecimal actualOutTrxAmount = buy.getActualTrxAmount(actualTokenAmount);
              long getTrx = actualGetTrxAmount.subtract(actualOutTrxAmount).multiply(TRX_DIVISOR).longValueExact();
              if (getTrx > 0) {
                targetAddr.addProfit(getTrx);
//                targetAddr.addSuccessAttackCount(1);
                targetAddr.addFuzzySuccessAttackCount(1);
              } else {
                targetAddr.addLoss(getTrx);
//                targetAddr.addFailAttackCount(1);
                targetAddr.addFuzzyFailAttackCount(1);
              }

              buy.subTokenAmount(actualTokenAmount);
              sell.subTokenAmount(actualTokenAmount);
              if (!buy.feeAdded) {
                feeToAdd = feeToAdd.add(buy.fee);
                buy.feeAdded = true;
                buyFeeAddedLastBlock++;
              }
              if (!sell.feeAdded) {
                feeToAdd = feeToAdd.add(sell.fee);
                sell.feeAdded = true;
                sellFeeAddedLastBlock++;
              }

              if (buy.isMatched()) {
                buyRecordIterator.remove();
              }

              if (sell.isMatched()) {
                sellRecordIterator.remove();
                break;
              }
            }
          }

          // 上个块卖可以平账，最近2块有卖有卖
          if (!sellsLastBlock.isEmpty()) {
            BigDecimal tokenAmount = BigDecimal.ZERO;
            BigDecimal trxAmount = BigDecimal.ZERO;
            for (SingleBuySellRecord sell : sellsLastBlock) {
              tokenAmount = tokenAmount.add(sell.getTokenAmount());
              trxAmount = trxAmount.add(sell.getTrxAmount());
            }
            // 平账
            tokenAllInfoRecord.removeRemaining(tokenAmount, trxAmount);
          }

          if (!buyLastBlock.isEmpty() && buySellsLastBlocks.remainingBuyAvailable) {
            // 全部匹配完还有剩余，记账
            BigDecimal tokenAmount = BigDecimal.ZERO;
            BigDecimal trxAmount = BigDecimal.ZERO;
            for (SingleBuySellRecord buy : buyLastBlock) {
              tokenAmount = tokenAmount.add(buy.getTokenAmount());
              trxAmount = trxAmount.add(buy.getTrxAmount());
            }
            // 记账
            tokenAllInfoRecord.addRemaining(tokenAmount, trxAmount);
          }

          long attackCountLastBlock =
              Math.min(
                  buySellsLastBlocks.availableAttackBuyCount(),
                  buySellsLastBlocks.availableAttackSellCount());
          targetAddr.addPairAttackCount(attackCountLastBlock);
          if (buySellsLastBlocks.attackTargetCount == 0 && buySellsLastBlocks.isAttacking()) {
//            targetAddr.addAttemptAttackCount(1);
            buySellsLastBlocks.attackTargetCount++;
          }

          // fee
          if (buyFeeAddedLastBlock < attackCountLastBlock) {
            for (SingleBuySellRecord buy : buySellsLastBlocks.records) {
              if (buy.isBuy && !buy.feeAdded) {
                feeToAdd = feeToAdd.add(buy.fee);
                buy.feeAdded = true;
                buyFeeAddedLastBlock++;
              }
              if (buyFeeAddedLastBlock == attackCountLastBlock) {
                break;
              }
            }
          }
          if (sellFeeAddedLastBlock < attackCountLastBlock) {
            for (SingleBuySellRecord sell : buySellsLastBlocks.records) {
              if (!sell.isBuy && !sell.feeAdded) {
                feeToAdd = feeToAdd.add(sell.fee);
                sell.feeAdded = true;
                sellFeeAddedLastBlock++;
              }
              if (sellFeeAddedLastBlock == attackCountLastBlock) {
                break;
              }
            }
          }
          // todo 整理
          if (buySellsLastBlocks.isAttacking()) {
            for (SingleBuySellRecord record : buySellsLastBlocks.records) {
              if (!record.feeAdded) {
                feeToAdd = feeToAdd.add(record.fee);
                record.feeAdded = true;
              }
            }
          }

          targetAddrAllInfoRecord.updateTokenAllInfoRecord(tokenEntry.getKey(), tokenAllInfoRecord);
        }
      }

      targetAddr.addAttackFee(feeToAdd.multiply(TRX_DIVISOR).longValueExact());
  //      if (addrBlockSuccess && toRecordSr) {
  //        addrAllInfoRecord.addWitness(witness);
  //      }
      targetAddr.setRemainingTokenValue(targetAddrAllInfoRecord.getTrxOutAmount().multiply(TRX_DIVISOR).longValueExact());
      chainBaseManager.getContractStateStore().setMevTPsRecord(targetAddr);
      // 删除1个块之前记录
      targetAddrContinusRecord.removeRecordByBlockNum(blockNum - 1);
    } catch (Exception e) {
      logger.error("Mev stat proccess block error", e);
    }
  }

  private static boolean matchBuySell(
      SingleBuySellRecord sellRecord,
      List<SingleBuySellRecord> buySells,
      ContractStateCapsule record,
      String token,
      int endIndex,
      boolean blockSuccess) {

    for (int i = 0; i < Math.min(endIndex, buySells.size()); i++) {
      SingleBuySellRecord toMatch = buySells.get(i);
      if (toMatch.isSuccess()
          && !toMatch.isMatched()
          && toMatch.isBuy
          && isMatch(toMatch.tokenAmount, sellRecord.tokenAmount)) {
        // 匹配上，计算获利和损失
        long getTrx = sellRecord.trxAmount.subtract(toMatch.trxAmount).multiply(TRX_DIVISOR).longValueExact();
        if (getTrx > 0) {
          record.addProfit(getTrx);
        } else {
          record.addLoss(getTrx);
        }
        toMatch.match();
        sellRecord.match();
        return getTrx > 0;
      }
    }
    return blockSuccess;
  }

  private static String get41Addr(String hexAddr) {
    if (!hexAddr.startsWith("41")) {
      return "41" + hexAddr;
    }
    return hexAddr;
  }

  private static Map<String, String> populateMap() throws IOException {
    // token pair map
    File file = new File("test.txt");
    BufferedReader reader = new BufferedReader(new FileReader(file));
    String line;
    Map<String, String> map = new HashMap<>();
    while ((line = reader.readLine()) != null) {
      String[] strings = line.split(" ");
      String token = strings[0];
      String pair = strings[1];
      long blockNum = Long.parseLong(strings[2]);
      long timestamp = Long.parseLong(strings[3]);

      map.put(
          Hex.toHexString(Commons.decodeFromBase58Check(pair)).substring(2),
          Hex.toHexString(Commons.decodeFromBase58Check(token)).substring(2));
    }
    return map;
  }

  private static boolean smallerToWtrx(String token, String wtrx) {
    return token.compareTo(wtrx) < 0;
  }

  // 结构体
  @AllArgsConstructor
  @Getter
  private static class SingleBuySellRecord {
    String txId;
    String caller;
    int index;
    String token;
    boolean isBuy;
    BigDecimal tokenAmount;
    BigDecimal trxAmount;
    long blockNum;
    long timestamp;
    String witness;
    boolean matched;
    boolean success;
    BigDecimal fee;
    boolean feeAdded;

    private SingleBuySellRecord(long blockNum, boolean success) {
      tokenAmount = BigDecimal.ZERO;
      trxAmount = BigDecimal.ZERO;
      this.blockNum = blockNum;
      matched = false;
      this.success = success;
      fee = BigDecimal.ZERO;
      feeAdded = false;
    }

    private BigDecimal getTokenPrice() {
      return trxAmount.divide(tokenAmount, 18, RoundingMode.HALF_UP);
    }

    private void match() {
      matched = true;
    }

    private BigDecimal getActualTrxAmount(BigDecimal actualTokenAmount) {
      if (actualTokenAmount.compareTo(tokenAmount) >= 0) {
        return trxAmount;
      }
      return trxAmount.multiply(actualTokenAmount).divide(tokenAmount, 6, RoundingMode.HALF_UP);
    }

    private void subTokenAmount(BigDecimal actualTokenAmount) {
      if (actualTokenAmount.compareTo(tokenAmount) >= 0
          || isMatch(actualTokenAmount, tokenAmount)) {
        tokenAmount = BigDecimal.ZERO;
        trxAmount = BigDecimal.ZERO;
        matched = true;
      } else {
        BigDecimal actualTrxAmount = getActualTrxAmount(actualTokenAmount);
        tokenAmount = tokenAmount.subtract(actualTokenAmount);
        trxAmount = trxAmount.subtract(actualTrxAmount);
      }
    }
  }

  @AllArgsConstructor
  @Getter
  private static class ContinusBlockRecord {
    List<SingleBuySellRecord> records;
    long buyCount;
    long sellCount;
    long recordBuyCount;
    long recordSellCount;
    boolean remainingSellAvailable;
    boolean remainingBuyAvailable;
    BigDecimal sellFeeRemaining;
    BigDecimal buyFeeRemaining;
    long attackTargetCount;
    boolean blockSuccess;

    private ContinusBlockRecord() {
      records = new ArrayList<>();
      buyCount = 0;
      sellCount = 0;
      recordBuyCount = 0;
      recordSellCount = 0;
      remainingSellAvailable = false;
      remainingBuyAvailable = false;
      sellFeeRemaining = BigDecimal.ZERO;
      buyFeeRemaining = BigDecimal.ZERO;
      attackTargetCount = 0;
      blockSuccess = false;
    }

    private void addRecord(SingleBuySellRecord singleBuySellRecord) {
      records.add(singleBuySellRecord);
    }

    private void clearAllSuccess() {
      records.removeIf(SingleBuySellRecord::isSuccess); // 清除成功的();
    }

    private void updateRecords(List<SingleBuySellRecord> newRecords) {
      records.clear();
      records.addAll(newRecords);
    }

    private BigDecimal getAllBuyTokenAmount() {
      return records.stream()
          .filter(record -> record.isBuy)
          .map(SingleBuySellRecord::getTokenAmount)
          .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private BigDecimal getAllTrxOutAmount() {
      return records.stream()
          .filter(record -> record.isBuy)
          .map(SingleBuySellRecord::getTrxAmount)
          .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private void removeMatchedAndUnsuccess() {
      records.removeIf(record -> record.matched || !record.success);
    }

    private void removeMatched() {
      records.removeIf(record -> record.matched);
    }

    private void addBuyCount() {
      buyCount++;
    }

    private void addSellCount() {
      sellCount++;
    }

    private void addAttackTarget() {
      attackTargetCount++;
    }

    private void addSellCount(long count) {
      sellCount += count;
    }

    private long getBuyCount() {
      return buyCount;
    }

    private long getSellCount() {
      return sellCount;
    }

    private boolean hasSell() {
      return records.stream().anyMatch(record -> !record.isBuy);
    }

    private boolean hasSuccessSell() {
      return records.stream().anyMatch(record -> record.isSuccess() && !record.isBuy);
    }

    private boolean hasBuy() {
      return records.stream().anyMatch(record -> record.isBuy);
    }

    private long availableAttackBuyCount() {
      return buyCount - recordBuyCount;
    }

    private long availableAttackSellCount() {
      return sellCount - recordSellCount;
    }

    private void addUnsuccessFee() {
      for (SingleBuySellRecord record : records) {
        if (!record.success) {
          if (record.isBuy) {
            buyFeeRemaining = buyFeeRemaining.add(record.fee);
          } else {
            sellFeeRemaining = sellFeeRemaining.add(record.fee);
          }
        }
      }
    }

    private boolean isAttacking() {
      return buyCount > 0 && sellCount > 0;
    }
  }

  @AllArgsConstructor
  @Getter
  private static class AddrContinusRecord {
    String caller;
    Map<Long, Map<String, ContinusBlockRecord>> records; // <token, ContinusBlockRecord>

    private AddrContinusRecord(String caller) {
      this.caller = caller;
      records = new HashMap<>();
    }

    private void addRecord(
        String txId,
        int index,
        long blockNum,
        long timestamp,
        String witness,
        String token,
        boolean isBuy,
        BigDecimal tokenAmount,
        BigDecimal trxAmount,
        boolean success,
        BigDecimal fee) {
      Map<String, ContinusBlockRecord> tokenContinusRecord =
          records.getOrDefault(blockNum, new HashMap<>());

      ContinusBlockRecord record =
          tokenContinusRecord.getOrDefault(token, new ContinusBlockRecord());
      // todo token
      SingleBuySellRecord singleBuySellRecord =
          new SingleBuySellRecord(
              txId,
              caller,
              index,
              null,
              isBuy,
              tokenAmount,
              trxAmount,
              blockNum,
              timestamp,
              witness,
              false,
              success,
              fee,
              false);
      record.addRecord(singleBuySellRecord);
      tokenContinusRecord.put(token, record);
      records.put(blockNum, tokenContinusRecord);
    }

    private Map<String, ContinusBlockRecord> getRecordsByBlockNum(long thisBlockNum) {
      return records.getOrDefault(thisBlockNum, new HashMap<>());
    }

    private void updateRecordsByBlockNum(
        long thisBlockNum, Map<String, ContinusBlockRecord> record) {
      records.put(thisBlockNum, record);
    }

    private void removeRecordByBlockNum(long blockNum) {
      records.remove(blockNum);
    }
  }

  @AllArgsConstructor
  @Getter
  private static class TokenAllInfoRecord {
    String token;
    BigDecimal profit;
    BigDecimal lack;
    long successCount;
    long lackCount;
    BigDecimal remainingTokenAmount;
    BigDecimal trxOutAmount;
    long attackCount;
    long attackTargetCount;
    long mzsuccessCount;
    long mzlackCount;

    private TokenAllInfoRecord(String token) {
      this.token = token;
      profit = BigDecimal.ZERO;
      lack = BigDecimal.ZERO;
      successCount = 0;
      lackCount = 0;
      remainingTokenAmount = BigDecimal.ZERO;
      trxOutAmount = BigDecimal.ZERO;
      attackCount = 0;
      attackTargetCount = 0;
    }

    private void addTrxDiff(BigDecimal trxDiff, boolean mzzz) {
      if (trxDiff.compareTo(BigDecimal.ZERO) > 0) {
        profit = profit.add(trxDiff);
        successCount++;
        if (mzzz) {
          mzsuccessCount++;
        }
      } else {
        lack = lack.add(trxDiff);
        lackCount++;
        if (mzzz) {
          mzlackCount++;
        }
      }
    }

    private void addRemaining(BigDecimal tokenAmount, BigDecimal trxOutAmount) {
      remainingTokenAmount = remainingTokenAmount.add(tokenAmount);
      this.trxOutAmount = this.trxOutAmount.add(trxOutAmount);
    }

    private void addAttack(long count) {
      attackCount += count;
    }

    private void addAttackTarget(long count) {
      attackTargetCount += count;
    }

    private void removeRemaining(BigDecimal tokenSellAmount, BigDecimal trxGetAmount) {
      BigDecimal actualTokenSellAmount =
          tokenSellAmount.compareTo(remainingTokenAmount) > 0
              ? remainingTokenAmount
              : tokenSellAmount;
      BigDecimal actualTrxGetAmount =
          trxGetAmount
              .multiply(actualTokenSellAmount)
              .divide(tokenSellAmount, 6, RoundingMode.HALF_EVEN);
      remainingTokenAmount = remainingTokenAmount.subtract(actualTokenSellAmount);
      trxOutAmount = trxOutAmount.subtract(actualTrxGetAmount);
    }
  }

  @AllArgsConstructor
  @Getter
  private static class AddrAllInfoRecord {
    String caller;
    long buyCount;
    BigDecimal fee;
    Map<String, TokenAllInfoRecord> tokenRecords; // <token, TokenAllInfoRecord>
    long successBuy;
    long successSell;
    long failBuy;
    long failSell;
    BigDecimal allfee;
    Map<String, Long> successSrBlock;

    private AddrAllInfoRecord(String caller) {
      this.caller = caller;
      buyCount = 0;
      tokenRecords = new HashMap<>();
      fee = BigDecimal.ZERO;
      successBuy = 0;
      successSell = 0;
      failBuy = 0;
      failSell = 0;
      allfee = BigDecimal.ZERO;
      successSrBlock = new HashMap<>();
    }

    private void addBuy() {
      buyCount++;
    }

    private void addTokenRecord(String token, BigDecimal trxDiff) {
      addTokenRecord(token, trxDiff, false);
    }

    private void addTokenRecord(String token, BigDecimal trxDiff, boolean mzzz) {

      TokenAllInfoRecord tokenAllInfoRecord =
          tokenRecords.getOrDefault(token, new TokenAllInfoRecord(token));
      tokenAllInfoRecord.addTrxDiff(trxDiff, mzzz);
      tokenRecords.put(token, tokenAllInfoRecord);
    }

    private TokenAllInfoRecord getTokenAllInfoRecord(String token) {
      return tokenRecords.getOrDefault(token, new TokenAllInfoRecord(token));
    }

    private void updateTokenAllInfoRecord(String token, TokenAllInfoRecord tokenAllInfoRecord) {
      tokenRecords.put(token, tokenAllInfoRecord);
    }

    private BigDecimal getAllProfit() {
      return tokenRecords.values().stream()
          .map(TokenAllInfoRecord::getProfit)
          .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private BigDecimal getAllLack() {
      return tokenRecords.values().stream()
          .map(TokenAllInfoRecord::getLack)
          .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private long getAllAttack() {
      return tokenRecords.values().stream().mapToLong(TokenAllInfoRecord::getAttackCount).sum();
    }

    private long getAllAttackTarget() {
      return tokenRecords.values().stream()
          .mapToLong(TokenAllInfoRecord::getAttackTargetCount)
          .sum();
    }

    private long getSuccessCount() {
      return tokenRecords.values().stream().mapToLong(TokenAllInfoRecord::getSuccessCount).sum();
    }

    private long getLackCount() {
      return tokenRecords.values().stream().mapToLong(TokenAllInfoRecord::getLackCount).sum();
    }

    private BigDecimal getRemainingTokenAmount() {
      return tokenRecords.values().stream()
          .map(TokenAllInfoRecord::getRemainingTokenAmount)
          .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private BigDecimal getTrxOutAmount() {
      return tokenRecords.values().stream()
          .map(TokenAllInfoRecord::getTrxOutAmount)
          .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private long getMzSucCount() {
      return tokenRecords.values().stream().mapToLong(TokenAllInfoRecord::getMzsuccessCount).sum();
    }

    private long getMzLackCount() {
      return tokenRecords.values().stream().mapToLong(TokenAllInfoRecord::getMzlackCount).sum();
    }

    private BigDecimal addFee(BigDecimal newfee) {
      return this.fee = this.fee.add(newfee);
    }

    private void addWitness(String witness) {
      successSrBlock.put(witness, successSrBlock.getOrDefault(witness, 0L) + 1);
    }
  }

  private static boolean isMatch(BigDecimal first, BigDecimal second) {
    BigDecimal difference = first.subtract(second).abs(); // 计算差的绝对值
    BigDecimal threshold = new BigDecimal("1"); // 设置阈值为 1

    // 比较绝对值是否小于 1
    return difference.compareTo(threshold) <= 0;
  }

  /**
   * Generate a block.
   */
  public BlockCapsule generateBlock(Miner miner, long blockTime, long timeout) {
    String address =  StringUtil.encode58Check(miner.getWitnessAddress().toByteArray());
    final Histogram.Timer timer = Metrics.histogramStartTimer(
        MetricKeys.Histogram.BLOCK_GENERATE_LATENCY, address);
    Metrics.histogramObserve(MetricKeys.Histogram.MINER_DELAY,
        (System.currentTimeMillis() - blockTime) / Metrics.MILLISECONDS_PER_SECOND, address);
    long postponedTrxCount = 0;
    logger.info("Generate block {} begin.", chainBaseManager.getHeadBlockNum() + 1);

    BlockCapsule blockCapsule = new BlockCapsule(chainBaseManager.getHeadBlockNum() + 1,
        chainBaseManager.getHeadBlockId(),
        blockTime, miner.getWitnessAddress());
    blockCapsule.generatedByMyself = true;
    session.reset();
    session.setValue(revokingStore.buildSession());

    accountStateCallBack.preExecute(blockCapsule);

    if (getDynamicPropertiesStore().getAllowMultiSign() == 1) {
      byte[] privateKeyAddress = miner.getPrivateKeyAddress().toByteArray();
      AccountCapsule witnessAccount = getAccountStore()
          .get(miner.getWitnessAddress().toByteArray());
      if (!Arrays.equals(privateKeyAddress, witnessAccount.getWitnessPermissionAddress())) {
        logger.warn("Witness permission is wrong.");
        return null;
      }
    }

    Set<String> accountSet = new HashSet<>();
    AtomicInteger shieldedTransCounts = new AtomicInteger(0);
    List<TransactionCapsule> toBePacked = new ArrayList<>();
    long currentSize = blockCapsule.getInstance().getSerializedSize();
    boolean isSort = Args.getInstance().isOpenTransactionSort();
    while (pendingTransactions.size() > 0 || rePushTransactions.size() > 0) {
      boolean fromPending = false;
      TransactionCapsule trx;
      if (pendingTransactions.size() > 0) {
        trx = pendingTransactions.peek();
        if (isSort) {
          TransactionCapsule trxRepush = rePushTransactions.peek();
          if (trxRepush == null || trx.getOrder() >= trxRepush.getOrder()) {
            fromPending = true;
          } else {
            trx = rePushTransactions.poll();
            Metrics.gaugeInc(MetricKeys.Gauge.MANAGER_QUEUE, -1,
                MetricLabels.Gauge.QUEUE_REPUSH);
          }
        } else {
          fromPending = true;
        }
      } else {
        trx = rePushTransactions.poll();
        Metrics.gaugeInc(MetricKeys.Gauge.MANAGER_QUEUE, -1,
            MetricLabels.Gauge.QUEUE_REPUSH);
      }

      if (fromPending) {
        pendingTransactions.poll();
        Metrics.gaugeInc(MetricKeys.Gauge.MANAGER_QUEUE, -1,
                MetricLabels.Gauge.QUEUE_PENDING);
      }

      if (trx == null) {
        //  transaction may be removed by rePushLoop.
        logger.warn("Trx is null, fromPending: {}, pending: {}, repush: {}.",
                fromPending, pendingTransactions.size(), rePushTransactions.size());
        continue;
      }
      if (System.currentTimeMillis() > timeout) {
        logger.warn("Processing transaction time exceeds the producing time {}.",
            System.currentTimeMillis());
        break;
      }

      // check the block size
      long trxPackSize = trx.computeTrxSizeForBlockMessage();
      if ((currentSize + trxPackSize)
          > ChainConstant.BLOCK_SIZE) {
        postponedTrxCount++;
        continue; // try pack more small trx
      }
      //shielded transaction
      Transaction transaction = trx.getInstance();
      if (isShieldedTransaction(transaction)
          && shieldedTransCounts.incrementAndGet() > SHIELDED_TRANS_IN_BLOCK_COUNTS) {
        continue;
      }
      //multi sign transaction
      byte[] owner = trx.getOwnerAddress();
      String ownerAddress = ByteArray.toHexString(owner);
      if (accountSet.contains(ownerAddress)) {
        continue;
      } else {
        if (isMultiSignTransaction(transaction)) {
          accountSet.add(ownerAddress);
        }
      }
      if (ownerAddressSet.contains(ownerAddress)) {
        trx.setVerified(false);
      }
      // apply transaction
      try (ISession tmpSession = revokingStore.buildSession()) {
        accountStateCallBack.preExeTrans();
        processTransaction(trx, blockCapsule, true);
        accountStateCallBack.exeTransFinish();
        tmpSession.merge();
        toBePacked.add(trx);
        currentSize += trxPackSize;
      } catch (Exception e) {
        logger.warn("Process trx {} failed when generating block {}, {}.", trx.getTransactionId(),
            blockCapsule.getNum(), e.getMessage());
      }
    }
    blockCapsule.addAllTransactions(toBePacked);
    accountStateCallBack.executeGenerateFinish();

    session.reset();

    blockCapsule.setMerkleRoot();
    blockCapsule.sign(miner.getPrivateKey());

    BlockCapsule capsule = new BlockCapsule(blockCapsule.getInstance());
    capsule.generatedByMyself = true;
    Metrics.histogramObserve(timer);
    logger.info("Generate block {} success, trxs:{}, pendingCount: {}, rePushCount: {},"
                    + " postponedCount: {}, blockSize: {} B",
            capsule.getNum(), capsule.getTransactions().size(),
            pendingTransactions.size(), rePushTransactions.size(), postponedTrxCount,
            capsule.getSerializedSize());
    return capsule;
  }

  private void filterOwnerAddress(TransactionCapsule transactionCapsule, Set<String> result) {
    byte[] owner = transactionCapsule.getOwnerAddress();
    String ownerAddress = ByteArray.toHexString(owner);
    if (ownerAddressSet.contains(ownerAddress)) {
      result.add(ownerAddress);
    }
  }

  private boolean isMultiSignTransaction(Transaction transaction) {
    Contract contract = transaction.getRawData().getContract(0);
    switch (contract.getType()) {
      case AccountPermissionUpdateContract: {
        return true;
      }
      default:
    }
    return false;
  }

  private boolean isShieldedTransaction(Transaction transaction) {
    Contract contract = transaction.getRawData().getContract(0);
    switch (contract.getType()) {
      case ShieldedTransferContract: {
        return true;
      }
      default:
        return false;
    }
  }

  public TransactionStore getTransactionStore() {
    return chainBaseManager.getTransactionStore();
  }

  public TransactionHistoryStore getTransactionHistoryStore() {
    return chainBaseManager.getTransactionHistoryStore();
  }

  public TransactionRetStore getTransactionRetStore() {
    return chainBaseManager.getTransactionRetStore();
  }

  public BlockStore getBlockStore() {
    return chainBaseManager.getBlockStore();
  }

  /**
   * process block.
   */
  private void processBlock(BlockCapsule block, List<TransactionCapsule> txs)
      throws ValidateSignatureException, ContractValidateException, ContractExeException,
      AccountResourceInsufficientException, TaposException, TooBigTransactionException,
      DupTransactionException, TransactionExpirationException, ValidateScheduleException,
      ReceiptCheckErrException, VMIllegalException, TooBigTransactionResultException,
      ZksnarkException, BadBlockException, EventBloomException {
    // todo set revoking db max size.

    // checkWitness
    if (!consensus.validBlock(block)) {
      throw new ValidateScheduleException("validateWitnessSchedule error");
    }

    chainBaseManager.getBalanceTraceStore().initCurrentBlockBalanceTrace(block);

    //reset BlockEnergyUsage
    chainBaseManager.getDynamicPropertiesStore().saveBlockEnergyUsage(0);
    //parallel check sign
    if (!block.generatedByMyself) {
      try {
        preValidateTransactionSign(txs);
      } catch (InterruptedException e) {
        logger.error("Parallel check sign interrupted exception! block info: {}.", block, e);
        Thread.currentThread().interrupt();
      }
    }

    TransactionRetCapsule transactionRetCapsule =
        new TransactionRetCapsule(block);
    try {
      merkleContainer.resetCurrentMerkleTree();
      accountStateCallBack.preExecute(block);
      List<TransactionInfo> results = new ArrayList<>();
      long num = block.getNum();
      for (TransactionCapsule transactionCapsule : block.getTransactions()) {
        transactionCapsule.setBlockNum(num);
        if (block.generatedByMyself) {
          transactionCapsule.setVerified(true);
        }
        accountStateCallBack.preExeTrans();
        TransactionInfo result = processTransaction(transactionCapsule, block, true);
        accountStateCallBack.exeTransFinish();
        if (Objects.nonNull(result)) {
          results.add(result);
        }
      }
      proceeToBlock(num);
      transactionRetCapsule.addAllTransactionInfos(results);
      accountStateCallBack.executePushFinish();
    } finally {
      accountStateCallBack.exceptionFinish();
    }
    merkleContainer.saveCurrentMerkleTreeAsBestMerkleTree(block.getNum());
    block.setResult(transactionRetCapsule);
    if (getDynamicPropertiesStore().getAllowAdaptiveEnergy() == 1) {
      EnergyProcessor energyProcessor = new EnergyProcessor(
          chainBaseManager.getDynamicPropertiesStore(), chainBaseManager.getAccountStore());
      energyProcessor.updateTotalEnergyAverageUsage();
      energyProcessor.updateAdaptiveTotalEnergyLimit();
    }

    payReward(block);

    boolean flag = chainBaseManager.getDynamicPropertiesStore().getNextMaintenanceTime()
        <= block.getTimeStamp();
    if (flag) {
      targetAddrAllInfoRecord = new AddrAllInfoRecord("");
      try {
        cexAddrs = getTronCexAddresses();
      } catch (Exception e) {

      }
      proposalController.processProposals();
      chainBaseManager.getForkController().reset();

      // Do cycle stats within day
      long cycleNum = getDynamicPropertiesStore().getCurrentCycleNumber();
      System.out.println(
          "Sync to cycle: "
              + cycleNum
              + ", timestamp: "
              + block.getTimeStamp()
              + ", cur timestamp: "
              + System.currentTimeMillis());
      ContractStateCapsule usdt = chainBaseManager.getContractStateStore().getUsdtRecord();
      if (usdt != null) {
        System.out.println(
            usdt.getTransferCount()
                + " "
                + usdt.getTransferFromCount()
                + " "
                + usdt.getTransferFee()
                + " "
                + usdt.getTransferFromFee()
                + " "
                + usdt.getTransferEnergyUsage()
                + " "
                + usdt.getTransferFromEnergyUsage());
      }
    }

    if (!consensus.applyBlock(block)) {
      throw new BadBlockException("consensus apply block failed");
    }

    if (flag) {
      chainBaseManager.getForkController().reset();
    }

//    updateTransHashCache(block);
//    updateRecentBlock(block);
//    updateRecentTransaction(block);
    updateDynamicProperties(block);

    chainBaseManager.getBalanceTraceStore().resetCurrentBlockTrace();

    if (CommonParameter.getInstance().isJsonRpcFilterEnabled()) {
      Bloom blockBloom = chainBaseManager.getSectionBloomStore()
          .initBlockSection(transactionRetCapsule);
      chainBaseManager.getSectionBloomStore().write(block.getNum());
      block.setBloom(blockBloom);
    }
  }

  private void payReward(BlockCapsule block) {
    WitnessCapsule witnessCapsule =
        chainBaseManager.getWitnessStore().getUnchecked(block.getInstance().getBlockHeader()
            .getRawData().getWitnessAddress().toByteArray());
    if (getDynamicPropertiesStore().allowChangeDelegation()) {
      mortgageService.payBlockReward(witnessCapsule.getAddress().toByteArray(),
          getDynamicPropertiesStore().getWitnessPayPerBlock());
      mortgageService.payStandbyWitness();

      if (chainBaseManager.getDynamicPropertiesStore().supportTransactionFeePool()) {
        long transactionFeeReward = Math
            .floorDiv(chainBaseManager.getDynamicPropertiesStore().getTransactionFeePool(),
                Constant.TRANSACTION_FEE_POOL_PERIOD);
        mortgageService.payTransactionFeeReward(witnessCapsule.getAddress().toByteArray(),
            transactionFeeReward);
        chainBaseManager.getDynamicPropertiesStore().saveTransactionFeePool(
            chainBaseManager.getDynamicPropertiesStore().getTransactionFeePool()
                - transactionFeeReward);
      }
    } else {
      byte[] witness = block.getWitnessAddress().toByteArray();
      AccountCapsule account = getAccountStore().get(witness);
      account.setAllowance(account.getAllowance()
          + chainBaseManager.getDynamicPropertiesStore().getWitnessPayPerBlock());

      if (chainBaseManager.getDynamicPropertiesStore().supportTransactionFeePool()) {
        long transactionFeeReward = Math
            .floorDiv(chainBaseManager.getDynamicPropertiesStore().getTransactionFeePool(),
                Constant.TRANSACTION_FEE_POOL_PERIOD);
        account.setAllowance(account.getAllowance() + transactionFeeReward);
        chainBaseManager.getDynamicPropertiesStore().saveTransactionFeePool(
            chainBaseManager.getDynamicPropertiesStore().getTransactionFeePool()
                - transactionFeeReward);
      }

      getAccountStore().put(account.createDbKey(), account);
    }
  }

  private void postSolidityLogContractTrigger(Long blockNum, Long lastSolidityNum) {
    if (blockNum > lastSolidityNum) {
      return;
    }
    BlockingQueue contractLogTriggersQueue = Args.getSolidityContractLogTriggerMap()
        .get(blockNum);
    while (!contractLogTriggersQueue.isEmpty()) {
      ContractLogTrigger triggerCapsule = (ContractLogTrigger) contractLogTriggersQueue.poll();
      if (triggerCapsule == null) {
        break;
      }
      if (containsTransaction(ByteArray.fromHexString(triggerCapsule
          .getTransactionId()))) {
        triggerCapsule.setTriggerName(Trigger.SOLIDITYLOG_TRIGGER_NAME);
        EventPluginLoader.getInstance().postSolidityLogTrigger(triggerCapsule);
      } else {
        // when switch fork, block will be post to triggerCapsuleQueue, transaction may be not found
        logger.error("PostSolidityLogContractTrigger txId = {} not contains transaction.",
            triggerCapsule.getTransactionId());
      }
    }
    Args.getSolidityContractLogTriggerMap().remove(blockNum);
  }

  private void postSolidityEventContractTrigger(Long blockNum, Long lastSolidityNum) {
    if (blockNum > lastSolidityNum) {
      return;
    }
    BlockingQueue contractEventTriggersQueue = Args.getSolidityContractEventTriggerMap()
        .get(blockNum);
    while (!contractEventTriggersQueue.isEmpty()) {
      ContractEventTrigger triggerCapsule = (ContractEventTrigger) contractEventTriggersQueue
          .poll();
      if (triggerCapsule == null) {
        break;
      }
      if (containsTransaction(ByteArray.fromHexString(triggerCapsule
          .getTransactionId()))) {
        triggerCapsule.setTriggerName(Trigger.SOLIDITYEVENT_TRIGGER_NAME);
        EventPluginLoader.getInstance().postSolidityEventTrigger(triggerCapsule);
      }
    }
    Args.getSolidityContractEventTriggerMap().remove(blockNum);
  }

  private void updateTransHashCache(BlockCapsule block) {
    for (TransactionCapsule transactionCapsule : block.getTransactions()) {
      this.transactionIdCache.put(transactionCapsule.getTransactionId(), true);
    }
  }

  public void updateRecentBlock(BlockCapsule block) {
    chainBaseManager.getRecentBlockStore().put(ByteArray.subArray(
        ByteArray.fromLong(block.getNum()), 6, 8),
        new BytesCapsule(ByteArray.subArray(block.getBlockId().getBytes(), 8, 16)));
  }

  public void updateRecentTransaction(BlockCapsule block) {
    List list = new ArrayList<>();
    block.getTransactions().forEach(capsule -> {
      list.add(capsule.getTransactionId().toString());
    });
    RecentTransactionItem item = new RecentTransactionItem(block.getNum(), list);
    chainBaseManager.getRecentTransactionStore().put(
            ByteArray.subArray(ByteArray.fromLong(block.getNum()), 6, 8),
            new BytesCapsule(JsonUtil.obj2Json(item).getBytes()));
  }

  public void updateFork(BlockCapsule block) {
    int blockVersion = block.getInstance().getBlockHeader().getRawData().getVersion();
    if (blockVersion > ChainConstant.BLOCK_VERSION) {
      logger.warn("Newer block version found: {}, YOU MUST UPGRADE java-tron!", blockVersion);
    }
    chainBaseManager.getForkController().update(block);
  }

  public long getSyncBeginNumber() {
    logger.info("HeadNumber: {}, syncBeginNumber: {}, solidBlockNumber: {}.",
        chainBaseManager.getDynamicPropertiesStore().getLatestBlockHeaderNumber(),
        chainBaseManager.getDynamicPropertiesStore().getLatestBlockHeaderNumber()
            - revokingStore.size(),
        chainBaseManager.getDynamicPropertiesStore().getLatestSolidifiedBlockNum());
    return this.fetchSyncBeginNumber();
  }

  public long fetchSyncBeginNumber() {
    return chainBaseManager.getDynamicPropertiesStore().getLatestBlockHeaderNumber()
        - revokingStore.size();
  }

  public AssetIssueStore getAssetIssueStore() {
    return chainBaseManager.getAssetIssueStore();
  }


  public AssetIssueV2Store getAssetIssueV2Store() {
    return chainBaseManager.getAssetIssueV2Store();
  }

  public AccountIdIndexStore getAccountIdIndexStore() {
    return chainBaseManager.getAccountIdIndexStore();
  }

  public NullifierStore getNullifierStore() {
    return chainBaseManager.getNullifierStore();
  }

  public boolean isTooManyPending() {
    return getPendingTransactions().size() + getRePushTransactions().size()
        > maxTransactionPendingSize;
  }

  private void preValidateTransactionSign(List<TransactionCapsule> txs)
      throws InterruptedException, ValidateSignatureException {
    int transSize = txs.size();
    if (transSize <= 0) {
      return;
    }
    Histogram.Timer requestTimer = Metrics.histogramStartTimer(
        MetricKeys.Histogram.VERIFY_SIGN_LATENCY, MetricLabels.TRX);
    try {
      CountDownLatch countDownLatch = new CountDownLatch(transSize);
      List<Future<Boolean>> futures = new ArrayList<>(transSize);

      for (TransactionCapsule transaction : txs) {
        Future<Boolean> future = validateSignService
            .submit(new ValidateSignTask(transaction, countDownLatch, chainBaseManager));
        futures.add(future);
      }
      countDownLatch.await();

      for (Future<Boolean> future : futures) {
        try {
          future.get();
        } catch (ExecutionException e) {
          throw new ValidateSignatureException(e.getCause().getMessage());
        }
      }
    } finally {
      Metrics.histogramObserve(requestTimer);
    }
  }

  public void rePush(TransactionCapsule tx) {
    if (containsTransaction(tx)) {
      return;
    }

    try {
      this.pushTransaction(tx);
    } catch (ValidateSignatureException | ContractValidateException | ContractExeException
        | AccountResourceInsufficientException | VMIllegalException e) {
      logger.debug(e.getMessage(), e);
    } catch (DupTransactionException e) {
      logger.debug("Pending manager: dup trans", e);
    } catch (TaposException e) {
      logger.debug("Pending manager: tapos exception", e);
    } catch (TooBigTransactionException e) {
      logger.debug("Pending manager: too big transaction", e);
    } catch (TransactionExpirationException e) {
      logger.debug("Pending manager: expiration transaction", e);
    } catch (ReceiptCheckErrException e) {
      logger.debug("Pending manager: outOfSlotTime transaction", e);
    } catch (TooBigTransactionResultException e) {
      logger.debug("Pending manager: too big transaction result", e);
    }
  }

  public long getHeadBlockNum() {
    return getDynamicPropertiesStore().getLatestBlockHeaderNumber();
  }

  public void setCursor(Chainbase.Cursor cursor) {
    if (cursor == Chainbase.Cursor.PBFT) {
      long headNum = getHeadBlockNum();
      long pbftNum = chainBaseManager.getCommonDataBase().getLatestPbftBlockNum();
      revokingStore.setCursor(cursor, headNum - pbftNum);
    } else {
      revokingStore.setCursor(cursor);
    }
  }

  public void resetCursor() {
    revokingStore.setCursor(Chainbase.Cursor.HEAD, 0L);
  }

  private void startEventSubscribing() {

    try {
      eventPluginLoaded = EventPluginLoader.getInstance()
          .start(Args.getInstance().getEventPluginConfig());

      if (!eventPluginLoaded) {
        logger.error("Failed to load eventPlugin.");
      }

      FilterQuery eventFilter = Args.getInstance().getEventFilter();
      if (!Objects.isNull(eventFilter)) {
        EventPluginLoader.getInstance().setFilterQuery(eventFilter);
      }

    } catch (Exception e) {
      logger.error("{}", e);
    }
  }

  private void postSolidityFilter(final long oldSolidNum, final long latestSolidifiedBlockNumber) {
    if (oldSolidNum >= latestSolidifiedBlockNumber) {
      logger.warn("Post solidity filter failed, oldSolidity: {} >= latestSolidity: {}.",
          oldSolidNum, latestSolidifiedBlockNumber);
      return;
    }

    List<BlockCapsule> capsuleList = getContinuousBlockCapsule(latestSolidifiedBlockNumber);
    for (BlockCapsule blockCapsule : capsuleList) {
      postBlockFilter(blockCapsule, true);
      postLogsFilter(blockCapsule, true, false);
    }
  }

  private void postSolidityTrigger(final long oldSolidNum, final long latestSolidifiedBlockNumber) {
    if (eventPluginLoaded && EventPluginLoader.getInstance().isSolidityLogTriggerEnable()) {
      for (Long i : Args.getSolidityContractLogTriggerMap().keySet()) {
        postSolidityLogContractTrigger(i, latestSolidifiedBlockNumber);
      }
    }

    if (eventPluginLoaded && EventPluginLoader.getInstance().isSolidityEventTriggerEnable()) {
      for (Long i : Args.getSolidityContractEventTriggerMap().keySet()) {
        postSolidityEventContractTrigger(i, latestSolidifiedBlockNumber);
      }
    }

    if (eventPluginLoaded && EventPluginLoader.getInstance().isSolidityTriggerEnable()) {
      List<BlockCapsule> capsuleList = getContinuousBlockCapsule(latestSolidifiedBlockNumber);
      for (BlockCapsule blockCapsule : capsuleList) {
        SolidityTriggerCapsule solidityTriggerCapsule
            = new SolidityTriggerCapsule(blockCapsule.getNum());//unique key
        solidityTriggerCapsule.setTimeStamp(blockCapsule.getTimeStamp());
        boolean result = triggerCapsuleQueue.offer(solidityTriggerCapsule);
        if (!result) {
          logger.info("Too many trigger, lost solidified trigger, block number: {}.",
              blockCapsule.getNum());
        }
      }
    }

    if (CommonParameter.getInstance().isJsonRpcHttpSolidityNodeEnable()) {
      postSolidityFilter(oldSolidNum, latestSolidifiedBlockNumber);
    }
    lastUsedSolidityNum = latestSolidifiedBlockNumber;
  }

  private void processTransactionTrigger(BlockCapsule newBlock) {
    List<TransactionCapsule> transactionCapsuleList = newBlock.getTransactions();

    // need to set eth compatible data from transactionInfoList
    if (EventPluginLoader.getInstance().isTransactionLogTriggerEthCompatible()
          && newBlock.getNum() != 0) {
      TransactionInfoList transactionInfoList = TransactionInfoList.newBuilder().build();
      TransactionInfoList.Builder transactionInfoListBuilder = TransactionInfoList.newBuilder();

      try {
        TransactionRetCapsule result = chainBaseManager.getTransactionRetStore()
            .getTransactionInfoByBlockNum(ByteArray.fromLong(newBlock.getNum()));

        if (!Objects.isNull(result) && !Objects.isNull(result.getInstance())) {
          result.getInstance().getTransactioninfoList().forEach(
              transactionInfoListBuilder::addTransactionInfo
          );

          transactionInfoList = transactionInfoListBuilder.build();
        }
      } catch (BadItemException e) {
        logger.error("PostBlockTrigger getTransactionInfoList blockNum = {}, error is {}.",
            newBlock.getNum(), e.getMessage());
      }

      if (transactionCapsuleList.size() == transactionInfoList.getTransactionInfoCount()) {
        long cumulativeEnergyUsed = 0;
        long cumulativeLogCount = 0;
        long energyUnitPrice = chainBaseManager.getDynamicPropertiesStore().getEnergyFee();

        for (int i = 0; i < transactionCapsuleList.size(); i++) {
          TransactionInfo transactionInfo = transactionInfoList.getTransactionInfo(i);
          TransactionCapsule transactionCapsule = transactionCapsuleList.get(i);
          // reset block num to ignore value is -1
          transactionCapsule.setBlockNum(newBlock.getNum());

          cumulativeEnergyUsed += postTransactionTrigger(transactionCapsule, newBlock, i,
              cumulativeEnergyUsed, cumulativeLogCount, transactionInfo, energyUnitPrice);

          cumulativeLogCount += transactionInfo.getLogCount();
        }
      } else {
        logger.error("PostBlockTrigger blockNum = {} has no transactions or {}.",
            newBlock.getNum(),
            "the sizes of transactionInfoList and transactionCapsuleList are not equal");
        for (TransactionCapsule e : newBlock.getTransactions()) {
          postTransactionTrigger(e, newBlock);
        }
      }
    } else {
      for (TransactionCapsule e : newBlock.getTransactions()) {
        postTransactionTrigger(e, newBlock);
      }
    }
  }

  private void reOrgLogsFilter() {
    if (CommonParameter.getInstance().isJsonRpcHttpFullNodeEnable()) {
      logger.info("Switch fork occurred, post reOrgLogsFilter.");

      try {
        BlockCapsule oldHeadBlock = chainBaseManager.getBlockById(
            getDynamicPropertiesStore().getLatestBlockHeaderHash());
        postLogsFilter(oldHeadBlock, false, true);
      } catch (BadItemException | ItemNotFoundException e) {
        logger.error("Block header hash does not exist or is bad: {}.",
            getDynamicPropertiesStore().getLatestBlockHeaderHash());
      }
    }
  }

  private void postBlockFilter(final BlockCapsule blockCapsule, boolean solidified) {
    BlockFilterCapsule blockFilterCapsule = new BlockFilterCapsule(blockCapsule, solidified);
    if (!filterCapsuleQueue.offer(blockFilterCapsule)) {
      logger.info("Too many filters, block filter lost: {}.", blockCapsule.getBlockId());
    }
  }

  private void postLogsFilter(final BlockCapsule blockCapsule, boolean solidified,
      boolean removed) {
    if (!blockCapsule.getTransactions().isEmpty()) {
      long blockNumber = blockCapsule.getNum();
      List<TransactionInfo> transactionInfoList = new ArrayList<>();

      try {
        TransactionRetCapsule result = chainBaseManager.getTransactionRetStore()
            .getTransactionInfoByBlockNum(ByteArray.fromLong(blockNumber));

        if (!Objects.isNull(result) && !Objects.isNull(result.getInstance())) {
          transactionInfoList.addAll(result.getInstance().getTransactioninfoList());
        }
      } catch (BadItemException e) {
        logger.error("ProcessLogsFilter getTransactionInfoList blockNum = {}, error is {}.",
            blockNumber, e.getMessage());
        return;
      }

      LogsFilterCapsule logsFilterCapsule = new LogsFilterCapsule(blockNumber,
          blockCapsule.getBlockId().toString(), blockCapsule.getBloom(), transactionInfoList,
          solidified, removed);

      if (!filterCapsuleQueue.offer(logsFilterCapsule)) {
        logger.info("Too many filters, logs filter lost: {}.", blockNumber);
      }
    }
  }

  private void postBlockTrigger(final BlockCapsule blockCapsule) {
    // post block and logs for jsonrpc
    if (CommonParameter.getInstance().isJsonRpcHttpFullNodeEnable()) {
      postBlockFilter(blockCapsule, false);
      postLogsFilter(blockCapsule, false, false);
    }

    // process block trigger
    long solidityBlkNum = getDynamicPropertiesStore().getLatestSolidifiedBlockNum();
    if (eventPluginLoaded && EventPluginLoader.getInstance().isBlockLogTriggerEnable()) {
      List<BlockCapsule> capsuleList = new ArrayList<>();
      if (EventPluginLoader.getInstance().isBlockLogTriggerSolidified()) {
        capsuleList = getContinuousBlockCapsule(solidityBlkNum);
      } else {
        capsuleList.add(blockCapsule);
      }

      for (BlockCapsule capsule : capsuleList) {
        BlockLogTriggerCapsule blockLogTriggerCapsule = new BlockLogTriggerCapsule(capsule);
        blockLogTriggerCapsule.setLatestSolidifiedBlockNumber(solidityBlkNum);
        if (!triggerCapsuleQueue.offer(blockLogTriggerCapsule)) {
          logger.info("Too many triggers, block trigger lost: {}.", capsule.getBlockId());
        }
      }
    }

    // process transaction trigger
    if (eventPluginLoaded && EventPluginLoader.getInstance().isTransactionLogTriggerEnable()) {
      List<BlockCapsule> capsuleList = new ArrayList<>();
      if (EventPluginLoader.getInstance().isTransactionLogTriggerSolidified()) {
        capsuleList = getContinuousBlockCapsule(solidityBlkNum);
      } else {
        // need to reset block
        capsuleList.add(blockCapsule);
      }

      for (BlockCapsule capsule : capsuleList) {
        processTransactionTrigger(capsule);
      }
    }
  }

  private List<BlockCapsule> getContinuousBlockCapsule(long solidityBlkNum) {
    List<BlockCapsule> capsuleList = new ArrayList<>();
    long start = lastUsedSolidityNum < 0 ? solidityBlkNum : (lastUsedSolidityNum + 1);
    if (solidityBlkNum > start) {
      logger.info("Continuous block start:{}, end:{}", start, solidityBlkNum);
    }
    for (long blockNum = start; blockNum <= solidityBlkNum; blockNum++) {
      try {
        BlockCapsule capsule = chainBaseManager.getBlockByNum(blockNum);
        capsuleList.add(capsule);
      } catch (Exception e) {
        logger.error("GetContinuousBlockCapsule getBlockByNum blkNum = {} except, error is {}.",
            solidityBlkNum, e.getMessage());
      }
    }
    return capsuleList;
  }

  // return energyUsageTotal of the current transaction
  // cumulativeEnergyUsed is the total of energy used before the current transaction
  private long postTransactionTrigger(final TransactionCapsule trxCap,
      final BlockCapsule blockCap, int index, long preCumulativeEnergyUsed,
      long cumulativeLogCount, final TransactionInfo transactionInfo, long energyUnitPrice) {
    TransactionLogTriggerCapsule trx = new TransactionLogTriggerCapsule(trxCap, blockCap,
        index, preCumulativeEnergyUsed, cumulativeLogCount, transactionInfo, energyUnitPrice);
    trx.setLatestSolidifiedBlockNumber(getDynamicPropertiesStore()
        .getLatestSolidifiedBlockNum());
    if (!triggerCapsuleQueue.offer(trx)) {
      logger.info("Too many triggers, transaction trigger lost: {}.", trxCap.getTransactionId());
    }

    return trx.getTransactionLogTrigger().getEnergyUsageTotal();
  }


  private void postTransactionTrigger(final TransactionCapsule trxCap,
      final BlockCapsule blockCap) {
    TransactionLogTriggerCapsule trx = new TransactionLogTriggerCapsule(trxCap, blockCap);
    trx.setLatestSolidifiedBlockNumber(getDynamicPropertiesStore()
        .getLatestSolidifiedBlockNum());
    if (!triggerCapsuleQueue.offer(trx)) {
      logger.info("Too many triggers, transaction trigger lost: {}.", trxCap.getTransactionId());
    }
  }

  private void reOrgContractTrigger() {
    if (eventPluginLoaded
        && (EventPluginLoader.getInstance().isContractEventTriggerEnable()
        || EventPluginLoader.getInstance().isContractLogTriggerEnable())) {
      logger.info("Switch fork occurred, post reOrgContractTrigger.");
      try {
        BlockCapsule oldHeadBlock = chainBaseManager.getBlockById(
            getDynamicPropertiesStore().getLatestBlockHeaderHash());
        for (TransactionCapsule trx : oldHeadBlock.getTransactions()) {
          postContractTrigger(trx.getTrxTrace(), true, oldHeadBlock.getBlockId().toString());
        }
      } catch (BadItemException | ItemNotFoundException e) {
        logger.error("Block header hash does not exist or is bad: {}.",
            getDynamicPropertiesStore().getLatestBlockHeaderHash());
      }
    }
  }

  private void postContractTrigger(final TransactionTrace trace, boolean remove, String blockHash) {
    boolean isContractTriggerEnable = EventPluginLoader.getInstance()
        .isContractEventTriggerEnable() || EventPluginLoader
        .getInstance().isContractLogTriggerEnable();
    boolean isSolidityContractTriggerEnable = EventPluginLoader.getInstance()
        .isSolidityEventTriggerEnable() || EventPluginLoader
        .getInstance().isSolidityLogTriggerEnable();
    if (eventPluginLoaded
        && (isContractTriggerEnable || isSolidityContractTriggerEnable)) {
      // be careful, trace.getRuntimeResult().getTriggerList() should never return null
      for (ContractTrigger trigger : trace.getRuntimeResult().getTriggerList()) {
        ContractTriggerCapsule contractTriggerCapsule = new ContractTriggerCapsule(trigger);
        contractTriggerCapsule.getContractTrigger().setRemoved(remove);
        contractTriggerCapsule.setLatestSolidifiedBlockNumber(getDynamicPropertiesStore()
            .getLatestSolidifiedBlockNum());
        contractTriggerCapsule.setBlockHash(blockHash);

        if (!triggerCapsuleQueue.offer(contractTriggerCapsule)) {
          logger.info("Too many triggers, contract log trigger lost: {}.",
              trigger.getTransactionId());
        }
      }
    }
  }

  private void prepareStoreFactory() {
    StoreFactory.init();
    StoreFactory.getInstance().setChainBaseManager(chainBaseManager);
  }

  public TransactionCapsule getTxFromPending(String txId) {
    AtomicReference<TransactionCapsule> transactionCapsule = new AtomicReference<>();
    Sha256Hash txHash = Sha256Hash.wrap(ByteArray.fromHexString(txId));
    pendingTransactions.forEach(tx -> {
      if (tx.getTransactionId().equals(txHash)) {
        transactionCapsule.set(tx);
        return;
      }
    });
    if (transactionCapsule.get() != null) {
      return transactionCapsule.get();
    }
    rePushTransactions.forEach(tx -> {
      if (tx.getTransactionId().equals(txHash)) {
        transactionCapsule.set(tx);
        return;
      }
    });
    return transactionCapsule.get();
  }

  public Collection<String> getTxListFromPending() {
    Set<String> result = new HashSet<>();
    pendingTransactions.forEach(tx -> {
      result.add(tx.getTransactionId().toString());
    });
    rePushTransactions.forEach(tx -> {
      result.add(tx.getTransactionId().toString());
    });
    return result;
  }

  public long getPendingSize() {
    long value = getPendingTransactions().size() + getRePushTransactions().size()
        + getPoppedTransactions().size();
    return value;
  }

  private void initLiteNode() {
    // When using bloom filter for transaction de-duplication,
    // it is possible to use trans for secondary confirmation.
    // Init trans db for liteNode if needed.
    long headNum = chainBaseManager.getDynamicPropertiesStore().getLatestBlockHeaderNumber();
    long recentBlockCount = chainBaseManager.getRecentBlockStore().size();
    long recentBlockStart = headNum - recentBlockCount + 1;
    boolean needInit = false;
    if (recentBlockStart == 0) {
      needInit = true;
    } else {
      try {
        chainBaseManager.getBlockByNum(recentBlockStart);
      } catch (ItemNotFoundException | BadItemException e) {
        needInit = true;
      }
    }

    if (needInit) {
      // copy transaction from recent-transaction to trans
      logger.info("Load trans for lite node.");

      TransactionCapsule item = new TransactionCapsule(Transaction.newBuilder().build());

      long transactionCount = 0;
      long minBlock = Long.MAX_VALUE;
      long maxBlock = Long.MIN_VALUE;
      for (Map.Entry<byte[], BytesCapsule> entry :
          chainBaseManager.getRecentTransactionStore()) {
        byte[] data = entry.getValue().getData();
        RecentTransactionItem trx =
            JsonUtil.json2Obj(new String(data), RecentTransactionItem.class);
        if (trx == null) {
          continue;
        }
        transactionCount += trx.getTransactionIds().size();
        long blockNum = trx.getNum();
        maxBlock = Math.max(maxBlock, blockNum);
        minBlock = Math.min(minBlock, blockNum);
        item.setBlockNum(blockNum);
        trx.getTransactionIds().forEach(
            tid -> chainBaseManager.getTransactionStore().put(Hex.decode(tid), item));
      }
      logger.info("Load trans complete, trans: {}, from = {}, to = {}.",
          transactionCount, minBlock, maxBlock);
    }
  }


  public void setBlockWaitLock(boolean waitFlag) {
    if (waitFlag) {
      blockWaitLock.incrementAndGet();
    } else {
      blockWaitLock.decrementAndGet();
    }
  }

  private boolean isBlockWaitingLock() {
    return blockWaitLock.get() > NO_BLOCK_WAITING_LOCK;
  }

  public void close() {
    stopRePushThread();
    stopRePushTriggerThread();
    EventPluginLoader.getInstance().stopPlugin();
    stopFilterProcessThread();
    stopValidateSignThread();
    chainBaseManager.shutdown();
    revokingStore.shutdown();
    session.reset();
  }

  private static class ValidateSignTask implements Callable<Boolean> {

    private TransactionCapsule trx;
    private CountDownLatch countDownLatch;
    private ChainBaseManager manager;

    ValidateSignTask(TransactionCapsule trx, CountDownLatch countDownLatch,
        ChainBaseManager manager) {
      this.trx = trx;
      this.countDownLatch = countDownLatch;
      this.manager = manager;
    }

    @Override
    public Boolean call() throws ValidateSignatureException {
      try {
        trx.validateSignature(manager.getAccountStore(), manager.getDynamicPropertiesStore());
      } catch (ValidateSignatureException e) {
        throw e;
      } finally {
        countDownLatch.countDown();
      }
      return true;
    }
  }
}
