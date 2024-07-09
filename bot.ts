import { gql, GraphQLClient } from "graphql-request";
import {
  Connection,
  Keypair,
  PublicKey,
  TransactionMessage,
  VersionedTransaction,
  ComputeBudgetProgram
} from '@solana/web3.js';
import {
  createAssociatedTokenAccountIdempotentInstruction,
  createCloseAccountInstruction,
  getAccount,
  Account,
  getAssociatedTokenAddress,
  RawAccount,
  TOKEN_PROGRAM_ID,
} from '@solana/spl-token';
import {
  Liquidity,
  LiquidityPoolKeysV4,
  LiquidityStateV4,
  Percent,
  Token,
  TokenAmount,
  MAINNET_PROGRAM_ID,
  LIQUIDITY_STATE_LAYOUT_V4
} from '@raydium-io/raydium-sdk';
import { MarketCache, PoolCache, SnipeListCache } from './cache';
import { PoolFilters } from './filters';
import { TransactionExecutor } from './transactions';
import { createPoolKeys, logger, NETWORK, sleep } from './helpers';
import { Mutex } from 'async-mutex';
import BN from 'bn.js';
import { WarpTransactionExecutor } from './transactions/warp-transaction-executor';
import { JitoTransactionExecutor } from './transactions/jito-rpc-transaction-executor';
import { MinimalMarketLayoutV3 } from './helpers/market';

export interface BotConfig {
  wallet: Keypair;
  checkRenounced: boolean;
  checkFreezable: boolean;
  checkBurned: boolean;
  minPoolSize: TokenAmount;
  maxPoolSize: TokenAmount;
  quoteToken: Token;
  quoteAmount: TokenAmount;
  quoteAta: PublicKey;
  oneTokenAtATime: boolean;
  useSnipeList: boolean;
  autoSell: boolean;
  autoBuyDelay: number;
  autoSellDelay: number;
  maxBuyRetries: number;
  maxSellRetries: number;
  unitLimit: number;
  unitPrice: number;
  takeProfit: number;
  stopLoss: number;
  buySlippage: number;
  sellSlippage: number;
  priceCheckDuration: number;
  filterCheckInterval: number;
  filterCheckDuration: number;
  consecutiveMatchCount: number;
  snipeListCache?: SnipeListCache;
  trailingTakeProfit: number;
  trailingStopLoss: number;
  solPrice: number; // Current SOL price in USD
  priceCheckInterval: number; // Interval between price checks in milliseconds
}


export class Bot {
  private graphQLClient: GraphQLClient;
  private readonly poolFilters: PoolFilters;
  private readonly mutex: Mutex;
  private sellExecutionCount = 0;
  public readonly isWarp: boolean = false;
  public readonly isJito: boolean = false;

  constructor(
    private readonly connection: Connection,
    private readonly marketStorage: MarketCache,
    private readonly poolStorage: PoolCache,
    private readonly txExecutor: TransactionExecutor,
    readonly config: BotConfig,
  ) {
    this.isWarp = txExecutor instanceof WarpTransactionExecutor;
    this.isJito = txExecutor instanceof JitoTransactionExecutor;

    this.mutex = new Mutex();
    this.poolFilters = new PoolFilters(connection, {
      quoteToken: this.config.quoteToken,
      minPoolSize: this.config.minPoolSize,
      maxPoolSize: this.config.maxPoolSize,
    });

    if (this.config.useSnipeList && !this.config.snipeListCache) {
      this.config.snipeListCache = new SnipeListCache();
      this.config.snipeListCache.init();

      this.config.snipeListCache.on('newToken', (mint: string) => {
        logger.info(`New token detected in snipe list: ${mint}`);
        this.buyNewListedToken(mint).catch(error => {
          logger.error(`Failed to buy new listed token: ${mint}`, error);
        });
      });
    }

    this.graphQLClient = new GraphQLClient('https://programs.shyft.to/v0/graphql/?api_key=L5bgzrP8PyVEKX7y', {
      method: 'POST',
      jsonSerializer: {
        parse: JSON.parse,
        stringify: JSON.stringify,
      },
    });
  }

  async validate(): Promise<boolean> {
    try {
      await getAccount(this.connection, this.config.quoteAta, this.connection.commitment);
    } catch (error) {
      logger.error(
        `${this.config.quoteToken.symbol} token account not found in wallet: ${this.config.wallet.publicKey.toString()}`,
      );
      return false;
    }

    return true;
  }

  private async queryLpByToken(token: string): Promise<LiquidityPoolKeysV4 | null> {
    const query = gql`
      query MyQuery($where: Raydium_LiquidityPoolv4_bool_exp) {
        Raydium_LiquidityPoolv4(where: $where) {
          baseDecimal
          baseMint
          baseVault
          lpMint
          lpVault
          marketId
          marketProgramId
          openOrders
          owner
          quoteDecimal
          quoteMint
          quoteVault
          targetOrders
          withdrawQueue
          pubkey
        }
      }
    `;

    const variables = {
      where: {
        _or: [
          { baseMint: { _eq: token } },
          { quoteMint: { _eq: token } },
        ]
      }
    };

    try {
      const data: any = await this.graphQLClient.request(query, variables);
      if (data.Raydium_LiquidityPoolv4.length > 0) {
        const pool = data.Raydium_LiquidityPoolv4[0];
        const isBase = pool.baseMint === token;
        return {
          id: new PublicKey(pool.pubkey),
          baseMint: new PublicKey(isBase ? token : this.config.quoteToken.mint),
          quoteMint: new PublicKey(isBase ? this.config.quoteToken.mint : token),
          lpMint: new PublicKey(pool.lpMint),
          baseDecimals: isBase ? parseInt(pool.baseDecimal) : this.config.quoteToken.decimals,
          quoteDecimals: isBase ? this.config.quoteToken.decimals : parseInt(pool.quoteDecimal),
          lpDecimals: 6,
          version: 4,
          programId: new PublicKey(pool.marketProgramId),
          authority: new PublicKey(pool.owner),
          openOrders: new PublicKey(pool.openOrders),
          targetOrders: new PublicKey(pool.targetOrders),
          baseVault: new PublicKey(pool.baseVault),
          quoteVault: new PublicKey(pool.quoteVault),
          withdrawQueue: new PublicKey(pool.withdrawQueue),
          lpVault: new PublicKey(pool.lpVault),
          marketVersion: 3,
          marketProgramId: new PublicKey(pool.marketProgramId),
          marketId: new PublicKey(pool.marketId),
          marketAuthority: PublicKey.default,
          marketBaseVault: PublicKey.default,
          marketQuoteVault: PublicKey.default,
          marketBids: PublicKey.default,
          marketAsks: PublicKey.default,
          marketEventQueue: PublicKey.default,
          lookupTableAccount: PublicKey.default
        };
      }
      return null;
    } catch (error) {
      logger.error({ error }, 'Error querying Shifty API');
      return null;
    }
  }

  public async buyNewListedToken(mint: string) {
    logger.info({ mint }, `New token added to snipe list, initiating buy...`);

    try {
      const poolKeys = await this.queryLpByToken(mint);

      if (!poolKeys) {
        logger.warn({ mint }, `Liquidity pool not found for newly listed token after querying Shifty API`);
        return;
      }

      logger.info({
        mint,
        poolId: poolKeys.id.toString(),
        baseMint: poolKeys.baseMint.toString(),
        quoteMint: poolKeys.quoteMint.toString(),
        baseDecimals: poolKeys.baseDecimals,
        quoteDecimals: poolKeys.quoteDecimals
      }, `Extracted liquidity pool data from Shifty API`);

      const tokenMint = new PublicKey(mint);
      const isBase = poolKeys.baseMint.equals(tokenMint);

      logger.info({ isBase, tokenMint: tokenMint.toString() }, `Determined token position in pool`);

      // Create a minimal LiquidityStateV4 object with only the essential properties
      const minimalPoolState: Partial<LiquidityStateV4> = {
        baseMint: poolKeys.baseMint,
        quoteMint: poolKeys.quoteMint,
        lpMint: poolKeys.lpMint,
        openOrders: poolKeys.openOrders,
        targetOrders: poolKeys.targetOrders,
        baseDecimal: new BN(poolKeys.baseDecimals),
        quoteDecimal: new BN(poolKeys.quoteDecimals),
        marketId: poolKeys.marketId,
        marketProgramId: poolKeys.marketProgramId,
        baseVault: poolKeys.baseVault,
        quoteVault: poolKeys.quoteVault,
        withdrawQueue: poolKeys.withdrawQueue,
        lpVault: poolKeys.lpVault,
      };

      logger.info({
        poolId: poolKeys.id.toString(),
        baseMint: minimalPoolState.baseMint?.toString() ?? 'undefined',
        quoteMint: minimalPoolState.quoteMint?.toString() ?? 'undefined',
        baseDecimal: minimalPoolState.baseDecimal?.toString() ?? 'undefined',
        quoteDecimal: minimalPoolState.quoteDecimal?.toString() ?? 'undefined'
      }, `Prepared minimal pool state for buy`);

      if (!minimalPoolState.baseMint || !minimalPoolState.quoteMint || !minimalPoolState.baseDecimal || !minimalPoolState.quoteDecimal) {
        logger.error({ mint }, `Invalid pool state, missing required properties`);
        return;
      }

      const tokenIn = isBase ? this.config.quoteToken : new Token(TOKEN_PROGRAM_ID, this.config.quoteToken.mint, this.config.quoteToken.decimals);
      const tokenOut = new Token(TOKEN_PROGRAM_ID, tokenMint, isBase ? poolKeys.baseDecimals : poolKeys.quoteDecimals);

      logger.info({
        tokenInMint: tokenIn.mint.toString(),
        tokenInDecimals: tokenIn.decimals,
        tokenOutMint: tokenOut.mint.toString(),
        tokenOutDecimals: tokenOut.decimals
      }, `Prepared token information for buy`);

      // Pass the correct tokens to the buy method
      await this.buy(
        poolKeys.id,
        minimalPoolState as LiquidityStateV4,
        tokenIn,
        tokenOut
      );
    } catch (error) {
      logger.error({ mint, error }, `Failed to buy newly listed token`);
    }
  }

  public async buy(
    accountId: PublicKey,
    poolState: Partial<LiquidityStateV4>,
    tokenIn: Token,
    tokenOut: Token
  ): Promise<void> {
    const tokenInMintStr = tokenIn.mint.toString();
    const tokenOutMintStr = tokenOut.mint.toString();
    logger.info({
      accountId: accountId.toString(),
      tokenInMint: tokenInMintStr,
      tokenOutMint: tokenOutMintStr
    }, `Processing new pool for buy...`);

    if (this.config.useSnipeList && this.config.snipeListCache) {
      if (!this.config.snipeListCache.isInList(tokenOutMintStr)) {
        logger.warn({
          tokenOutMint: tokenOutMintStr,
          snipeListContents: this.config.snipeListCache.getList()
        }, `Skipping buy because token is not in the snipe list`);
        return;
      }
    }

    if (this.config.autoBuyDelay > 0) {
      logger.debug({ mint: tokenOutMintStr }, `Waiting for ${this.config.autoBuyDelay} ms before buy`);
      await sleep(this.config.autoBuyDelay);
    }

    if (this.config.oneTokenAtATime) {
      if (this.mutex.isLocked() || this.sellExecutionCount > 0) {
        logger.debug(
          { mint: tokenOutMintStr },
          `Skipping buy because one token at a time is turned on and token is already being processed`,
        );
        return;
      }
      await this.mutex.acquire();
    }

    try {
      let marketState: MinimalMarketLayoutV3 | null = null;
      try {
        marketState = await this.marketStorage.get(poolState.marketId?.toString() || '');
      } catch (error) {
        logger.warn(`Failed to fetch market data: ${(error as Error).message}. Proceeding without market data.`);
      }

      const mintAta = await getAssociatedTokenAddress(tokenOut.mint, this.config.wallet.publicKey);

      let poolKeys: LiquidityPoolKeysV4;
      try {
        poolKeys = createPoolKeys(accountId, poolState, marketState);
        logger.debug(`Pool keys created successfully: ${JSON.stringify(poolKeys)}`);
      } catch (error) {
        logger.error(`Failed to create pool keys: ${(error as Error).message}`);
        return;
      }

      logger.info({
        poolKeys: JSON.stringify(poolKeys),
        market: marketState ? 'Found' : 'Not Found',
        mintAta: mintAta.toString()
      }, 'Prepared pool keys and associated token address');

      if (!this.config.useSnipeList) {
        const match = await this.filterMatch(poolKeys);
        if (!match) {
          logger.trace({ mint: tokenOutMintStr }, `Skipping buy because pool doesn't match filters`);
          return;
        }
      }

      for (let i = 0; i < this.config.maxBuyRetries; i++) {
        try {
          logger.info(
            { mint: tokenOutMintStr },
            `Send buy transaction attempt: ${i + 1}/${this.config.maxBuyRetries}`,
          );
          const result = await this.swap(
            poolKeys,
            this.config.quoteAta,
            mintAta,
            tokenIn,
            tokenOut,
            this.config.quoteAmount,
            this.config.buySlippage,
            this.config.wallet,
            'buy',
          );

          if (result.confirmed) {
            logger.info(
              {
                mint: tokenOutMintStr,
                signature: result.signature,
                url: `https://solscan.io/tx/${result.signature}?cluster=${NETWORK}`,
              },
              `Confirmed buy tx`,
            );

            const tokenAccount = await getAccount(this.connection, mintAta);
            const boughtTokens = new TokenAmount(tokenOut, tokenAccount.amount.toString(), false);

            const boughtTokensDisplay = parseFloat(boughtTokens.toExact()) / 10 ** tokenOut.decimals;
            const boughtAmountSol = this.config.quoteAmount.toExact();
            const purchasePrice = parseFloat(boughtAmountSol) / boughtTokensDisplay;

            logger.info(`Bought ${boughtTokensDisplay.toFixed(6)} ${tokenOut.symbol} for ${boughtAmountSol} SOL`);
            logger.info(`Purchase price calculation: ${boughtAmountSol} SOL / ${boughtTokensDisplay.toFixed(6)} tokens = ${purchasePrice.toFixed(9)} SOL per token`);

            await this.monitorPrice(
              poolKeys,
              boughtTokens,
              this.config.quoteAmount,
              new BN(purchasePrice * 1e9),
              mintAta,
              tokenOut
            );
            break;
          } else {
            logger.info(
              {
                mint: tokenOutMintStr,
                signature: result.signature,
                error: result.error,
              },
              `Error confirming buy tx`,
            );
          }
        } catch (error) {
          logger.error({ mint: tokenOutMintStr, error }, `Failed to buy token`);
          if (i < this.config.maxBuyRetries - 1) {
            logger.debug(`Waiting for 2 seconds before retrying...`);
            await sleep(2000);
          }
        }
      }
    } catch (error) {
      logger.error({ mint: tokenOutMintStr, error }, `Failed to buy token`);
    } finally {
      if (this.config.oneTokenAtATime) {
        this.mutex.release();
      }
    }
  }

  private async monitorPrice(
    poolKeys: LiquidityPoolKeysV4,
    boughtTokens: TokenAmount,
    quoteAmount: TokenAmount,
    purchasePrice: BN,
    mintAta: PublicKey,
    tokenOut: Token
  ): Promise<void> {
    let highestPrice = purchasePrice;
    let lowestPrice = purchasePrice;
    const initialStopLossPrice = purchasePrice.mul(new BN(100 - this.config.trailingStopLoss)).div(new BN(100));
    let trailingStopLossPrice = initialStopLossPrice;
    let trailingTakeProfitPrice = purchasePrice.mul(new BN(100 + this.config.trailingTakeProfit)).div(new BN(100));

    const boughtTokensDisplay = parseFloat(boughtTokens.toExact()) / 10 ** tokenOut.decimals;
    logger.info(`Starting price monitoring for ${boughtTokensDisplay.toFixed(6)} ${tokenOut.symbol}`);
    logger.info(`Purchase price: ${purchasePrice.toNumber() / 1e9} SOL per token`);
    logger.info(`Initial stop loss price: ${initialStopLossPrice.toNumber() / 1e9} SOL per token`);
    logger.info(`Initial take profit price: ${trailingTakeProfitPrice.toNumber() / 1e9} SOL per token`);

    let lastBaseReserve = new BN(0);
    let lastQuoteReserve = new BN(0);
    const PRICE_CHECK_DELAY = 5000; // 5 seconds

    while (true) {
      try {
        const poolInfo = await Liquidity.fetchInfo({
          connection: this.connection,
          poolKeys,
        });
        logger.info(`Successfully fetched pool info`);

        let tokenAccount: Account | null = null;
        try {
          tokenAccount = await getAccount(this.connection, mintAta);
          logger.info(`Token account exists. Balance: ${(parseFloat(tokenAccount.amount.toString()) / 10 ** tokenOut.decimals).toFixed(6)}`);
        } catch (error: unknown) {
          if (error instanceof Error && error.name === 'TokenAccountNotFoundError') {
            logger.warn(`Token account no longer exists. This might be a burnable token.`);
            break;
          } else {
            logger.error(`Error fetching token account: ${error instanceof Error ? error.message : 'Unknown error'}`);
            await sleep(PRICE_CHECK_DELAY);
            continue;
          }
        }

        if (!tokenAccount) {
          logger.warn(`Token account is null. Retrying in ${PRICE_CHECK_DELAY}ms.`);
          await sleep(PRICE_CHECK_DELAY);
          continue;
        }

        const baseReserve = new BN(poolInfo.baseReserve.toString());
        const quoteReserve = new BN(poolInfo.quoteReserve.toString());

        if (baseReserve.eq(lastBaseReserve) && quoteReserve.eq(lastQuoteReserve)) {
          logger.warn('Pool reserves have not changed since last check. This may indicate an issue with data fetching.');
        }

        lastBaseReserve = baseReserve;
        lastQuoteReserve = quoteReserve;

        const currentPrice = quoteReserve
          .mul(new BN(10 ** tokenOut.decimals))
          .div(baseReserve)
          .div(new BN(1)); // Divide by 1 to get the correct price

        const currentPriceNumber = currentPrice.toNumber() / 1e9;
        const purchasePriceNumber = purchasePrice.toNumber() / 1e9;

        logger.info(`Current price from Raydium: ${currentPriceNumber} SOL per token`);
        logger.info(`Base reserve: ${baseReserve.toString()}, Quote reserve: ${quoteReserve.toString()}`);

        if (currentPrice.gt(highestPrice)) {
          highestPrice = currentPrice;
          trailingTakeProfitPrice = highestPrice.mul(new BN(100 - this.config.trailingTakeProfit)).div(new BN(100));
          const newTrailingStopLoss = highestPrice.mul(new BN(100 - this.config.trailingStopLoss)).div(new BN(100));
          if (newTrailingStopLoss.gt(trailingStopLossPrice)) {
            trailingStopLossPrice = newTrailingStopLoss;
          }
        }
        if (currentPrice.lt(lowestPrice)) {
          lowestPrice = currentPrice;
        }

        // Correct delta calculation
        const delta = ((currentPriceNumber - purchasePriceNumber) / purchasePriceNumber) * 100;

        logger.info(`- position update -
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                     Active position: ${tokenOut.mint.toString()}                      │
├───────────────────────────┬───────────────────────────┬───────────────────────────┬────────────────────┤
│ purchase price            │ trailing stop loss        │ trailing take profit      │ current price      │
├───────────────────────────┼───────────────────────────┼───────────────────────────┼────────────────────┤
│ ${purchasePriceNumber.toFixed(9)} SOL │ ${(trailingStopLossPrice.toNumber() / 1e9).toFixed(9)} SOL │ ${(trailingTakeProfitPrice.toNumber() / 1e9).toFixed(9)} SOL │ ${currentPriceNumber.toFixed(9)} SOL │
├───────────────────────────┴───────────────────────────┴───────────────────────────┴────────────────────┤
│ delta: ${delta.toFixed(2)}% (formula: ((${currentPriceNumber.toFixed(9)} - ${purchasePriceNumber.toFixed(9)}) / ${purchasePriceNumber.toFixed(9)}) * 100)        │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘`);

        if (currentPrice.lte(trailingStopLossPrice)) {
          logger.info({ mint: tokenOut.mint.toString() }, `Trailing stop loss triggered`);
          await this.sell(mintAta, tokenAccount, poolKeys, tokenOut);
          return;
        }

        if (currentPrice.gte(trailingTakeProfitPrice)) {
          logger.info({ mint: tokenOut.mint.toString() }, `Trailing take profit triggered`);
          await this.sell(mintAta, tokenAccount, poolKeys, tokenOut);
          return;
        }

        await sleep(PRICE_CHECK_DELAY);
      } catch (e) {
        logger.error({
          mint: tokenOut.mint.toString(),
          error: e instanceof Error ? e.message : 'Unknown error',
          stack: e instanceof Error ? e.stack : 'No stack trace available'
        }, `Failed to check token price`);

        await sleep(PRICE_CHECK_DELAY);
      }
    }
  }


  public async sell(
    accountId: PublicKey,
    account: Account,
    poolKeys?: LiquidityPoolKeysV4,
    tokenOut?: Token
  ) {
    if (this.config.oneTokenAtATime) {
      this.sellExecutionCount++;
    }

    try {
      logger.info({
        mint: account.mint.toString(),
        amount: account.amount.toString(),
        accountId: accountId.toString(),
      }, `Initiating sell process...`);

      if (!poolKeys || !tokenOut) {
        logger.info(`PoolKeys or TokenOut not provided, attempting to fetch...`);
        const poolData = await this.poolStorage.get(account.mint.toString());
        if (!poolData) {
          logger.error({ mint: account.mint.toString() }, `Failed to fetch pool data for token`);
          return;
        }
        poolKeys = createPoolKeys(new PublicKey(poolData.id), poolData.state, null);
        tokenOut = new Token(TOKEN_PROGRAM_ID, new PublicKey(account.mint), poolData.state.baseDecimal.toNumber());
      }

      logger.info({
        tokenOutMint: tokenOut.mint.toString(),
        tokenOutDecimals: tokenOut.decimals
      }, `Token information`);

      const tokenAmountIn = new TokenAmount(tokenOut, account.amount.toString(), false);

      logger.info({
        tokenAmountIn: tokenAmountIn.toExact(),
        tokenAmountInRaw: tokenAmountIn.raw.toString()
      }, `Token amount to sell`);

      if (tokenAmountIn.isZero()) {
        logger.info({ mint: account.mint.toString() }, `Empty balance, can't sell`);
        return;
      }

      logger.info({
        poolKeysId: poolKeys.id.toString(),
        poolKeysBaseMint: poolKeys.baseMint.toString(),
        poolKeysQuoteMint: poolKeys.quoteMint.toString()
      }, `Pool keys information`);

      const maxRetries = 200;
      const initialRetryDelay = 100; // 100ms
      const maxRetryDelay = 5000; // 5 seconds

      for (let i = 0; i < maxRetries; i++) {
        try {
          logger.info(
            { mint: account.mint, attempt: i + 1, maxRetries },
            `Sending sell transaction`
          );

          const result = await this.swap(
            poolKeys,
            accountId,
            this.config.quoteAta,
            tokenOut,
            this.config.quoteToken,
            tokenAmountIn,
            this.config.sellSlippage,
            this.config.wallet,
            'sell',
          );

          if (result.confirmed) {
            logger.info(
              {
                dex: `https://dexscreener.com/solana/${account.mint.toString()}?maker=${this.config.wallet.publicKey}`,
                mint: account.mint.toString(),
                signature: result.signature,
                url: `https://solscan.io/tx/${result.signature}?cluster=${NETWORK}`,
              },
              `SELL CONFIRMED: Transaction successful`
            );
            return; // Exit the function after successful sell
          }

          logger.warn(
            {
              mint: account.mint.toString(),
              signature: result.signature,
              error: result.error,
            },
            `Sell transaction not confirmed, retrying...`
          );
        } catch (error) {
          logger.error({
            mint: account.mint.toString(),
            error: error instanceof Error ? error.message : 'Unknown error',
            stack: error instanceof Error ? error.stack : 'No stack trace'
          }, `Error executing sell transaction`);
        }

        if (i < maxRetries - 1) {
          const retryDelay = Math.min(initialRetryDelay * Math.pow(2, i), maxRetryDelay);
          logger.debug(`Waiting for ${retryDelay}ms before retrying...`);
          await sleep(retryDelay);
        }
      }

      logger.error({ mint: account.mint.toString() }, `SELL FAILED: Unable to sell token after ${maxRetries} attempts`);
    } catch (error) {
      logger.error({
        mint: account.mint.toString(),
        error: error instanceof Error ? error.message : 'Unknown error',
        stack: error instanceof Error ? error.stack : 'No stack trace'
      }, `Failed to sell token`);
    } finally {
      if (this.config.oneTokenAtATime) {
        this.sellExecutionCount--;
      }
      logger.info({ mint: account.mint.toString() }, `Completed sell process`);
    }
  }

  private async swap(
    poolKeys: LiquidityPoolKeysV4,
    ataIn: PublicKey,
    ataOut: PublicKey,
    tokenIn: Token,
    tokenOut: Token,
    amountIn: TokenAmount,
    slippage: number,
    wallet: Keypair,
    direction: 'buy' | 'sell',
  ): Promise<any> {
    try {
      logger.info({
        direction,
        tokenIn: tokenIn.symbol,
        tokenInMint: tokenIn.mint.toString(),
        tokenOut: tokenOut.symbol,
        tokenOutMint: tokenOut.mint.toString(),
        amountIn: amountIn.toExact(),
        ataIn: ataIn.toString(),
        ataOut: ataOut.toString(),
        slippage,
      }, `Initiating swap`);

      const slippagePercent = new Percent(slippage, 100);
      const poolInfo = await Liquidity.fetchInfo({
        connection: this.connection,
        poolKeys,
      });
      logger.debug(`Pool info fetched successfully`);

      const computedAmountOut = Liquidity.computeAmountOut({
        poolKeys,
        poolInfo,
        amountIn,
        currencyOut: tokenOut,
        slippage: slippagePercent,
      });
      logger.info({
        amountIn: amountIn.toExact(),
        expectedAmountOut: computedAmountOut.amountOut.toExact(),
        minAmountOut: computedAmountOut.minAmountOut.toExact(),
      }, `Computed swap amounts`);

      const latestBlockhash = await this.connection.getLatestBlockhash();

      // Convert BN to string to avoid potential overflow issues
      const amountInRaw = amountIn.raw.toString();
      const minAmountOutRaw = computedAmountOut.minAmountOut.raw.toString();

      const { innerTransaction } = Liquidity.makeSwapFixedInInstruction(
        {
          poolKeys: poolKeys,
          userKeys: {
            tokenAccountIn: ataIn,
            tokenAccountOut: ataOut,
            owner: wallet.publicKey,
          },
          amountIn: direction == "sell" ? amountIn.toExact().toString() : amountInRaw,
          minAmountOut: direction == "sell" ? 0 : minAmountOutRaw,
        },
        poolKeys.version,
      );
      logger.debug(`Swap instruction created`);

      const instructions = [
        ...(this.isWarp || this.isJito
          ? []
          : [
            ComputeBudgetProgram.setComputeUnitPrice({ microLamports: this.config.unitPrice }),
            ComputeBudgetProgram.setComputeUnitLimit({ units: this.config.unitLimit }),
          ]),
        ...(direction === 'buy'
          ? [
            createAssociatedTokenAccountIdempotentInstruction(
              wallet.publicKey,
              ataOut,
              wallet.publicKey,
              tokenOut.mint,
            ),
          ]
          : []),
        ...innerTransaction.instructions,
      ];

      if (direction === 'sell') {
        // Check if the account will be empty after the swap
        const tokenAccount = await this.connection.getTokenAccountBalance(ataIn);
        if (tokenAccount.value.uiAmount === parseFloat(amountIn.toExact())) {
          instructions.push(createCloseAccountInstruction(ataIn, wallet.publicKey, wallet.publicKey));
          logger.info(`Adding close account instruction for ${ataIn.toString()}`);
        }
      }

      const messageV0 = new TransactionMessage({
        payerKey: wallet.publicKey,
        recentBlockhash: latestBlockhash.blockhash,
        instructions,
      }).compileToV0Message();

      const transaction = new VersionedTransaction(messageV0);
      transaction.sign([wallet, ...innerTransaction.signers]);

      const simulateTxRes = await this.connection.simulateTransaction(transaction);
      if (simulateTxRes.value.err != null) {
        logger.trace('"Transaction not correct!...');
        logger.trace(simulateTxRes.value)
        return {
          confirmed: false,
          error: "simulate error"
        }
      } else {
        logger.trace('"Transaction is correct!...');
      }


      logger.debug('Sending transaction...');
      const result = await this.txExecutor.executeAndConfirm(transaction, wallet, latestBlockhash);

      if (result.confirmed) {
        logger.info({
          direction,
          tokenIn: tokenIn.symbol,
          tokenOut: tokenOut.symbol,
          amountIn: amountIn.toExact(),
          amountOut: computedAmountOut.minAmountOut.toExact(),
          signature: result.signature,
          url: `https://solscan.io/tx/${result.signature}?cluster=${NETWORK}`,
        }, `Swap transaction confirmed`);
      } else {
        logger.warn({
          direction,
          tokenIn: tokenIn.symbol,
          tokenOut: tokenOut.symbol,
          amountIn: amountIn.toExact(),
          amountOut: computedAmountOut.minAmountOut.toExact(),
          signature: result.signature,
          error: result.error,
        }, `Swap transaction failed to confirm`);
      }

      return result;
    } catch (error) {
      logger.error({
        error: error instanceof Error ? error.message : 'Unknown error',
        stack: error instanceof Error ? error.stack : 'No stack trace',
        direction,
        tokenIn: tokenIn.symbol,
        tokenOut: tokenOut.symbol,
        amountIn: amountIn.toExact(),
        poolKeys: poolKeys.id.toString()
      }, `Swap error`);

      if (direction === 'sell') {
        logger.error(`Sell-specific error details: ${JSON.stringify({
          poolKeys: poolKeys,
          ataIn: ataIn.toString(),
          ataOut: ataOut.toString(),
          tokenIn: tokenIn.symbol,
          tokenOut: tokenOut.symbol,
          amountIn: amountIn.toExact(),
          slippage: slippage
        })}`);
      }

      throw error;
    }
  }


  private async filterMatch(poolKeys: LiquidityPoolKeysV4): Promise<boolean> {
    if (this.config.filterCheckInterval === 0 || this.config.filterCheckDuration === 0) {
      return true;
    }

    const timesToCheck = this.config.filterCheckDuration / this.config.filterCheckInterval;
    let timesChecked = 0;
    let matchCount = 0;

    do {
      try {
        const shouldBuy = await this.poolFilters.execute(poolKeys);

        if (shouldBuy) {
          matchCount++;

          if (this.config.consecutiveMatchCount <= matchCount) {
            logger.debug(
              { mint: poolKeys.baseMint.toString() },
              `Filter match ${matchCount}/${this.config.consecutiveMatchCount}`,
            );
            return true;
          }
        } else {
          matchCount = 0;
        }

        await sleep(this.config.filterCheckInterval);
      } finally {
        timesChecked++;
      }
    } while (timesChecked < timesToCheck);

    return false;
  }

  private async priceMatch(amountIn: TokenAmount, poolKeys: LiquidityPoolKeysV4): Promise<void> {
    if (this.config.priceCheckDuration === 0 || this.config.priceCheckInterval === 0) {
      return;
    }

    const timesToCheck = this.config.priceCheckDuration / this.config.priceCheckInterval;
    let highestPrice = this.config.quoteAmount.raw;
    let lowestPrice = this.config.quoteAmount.raw;
    const slippage = new Percent(this.config.sellSlippage, 100);
    let timesChecked = 0;

    do {
      try {
        const poolInfo = await Liquidity.fetchInfo({
          connection: this.connection,
          poolKeys,
        });

        const amountOut = Liquidity.computeAmountOut({
          poolKeys,
          poolInfo,
          amountIn: amountIn,
          currencyOut: this.config.quoteToken,
          slippage,
        }).amountOut;

        const amountOutRaw = amountOut instanceof TokenAmount ? amountOut.raw : new BN(amountOut.numerator).div(new BN(amountOut.denominator));

        // Update highest and lowest prices
        if (amountOutRaw.gt(highestPrice)) {
          highestPrice = amountOutRaw;
        }
        if (amountOutRaw.lt(lowestPrice)) {
          lowestPrice = amountOutRaw;
        }

        // Calculate trailing take profit and stop loss
        const trailingTakeProfit = highestPrice.mul(new BN(100 - this.config.trailingTakeProfit)).div(new BN(100));
        const trailingStopLoss = lowestPrice.mul(new BN(100 + this.config.trailingStopLoss)).div(new BN(100));

        logger.debug(
          { mint: poolKeys.baseMint.toString() },
          `Trailing take profit: ${trailingTakeProfit.toString()} | Trailing stop loss: ${trailingStopLoss.toString()} | Current: ${amountOutRaw.toString()}`
        );

        if (amountOutRaw.lt(trailingStopLoss)) {
          logger.info({ mint: poolKeys.baseMint.toString() }, `Trailing stop loss triggered`);
          await this.sell(poolKeys.baseMint, await getAccount(this.connection, amountIn.token.mint));
          break;
        }

        if (amountOutRaw.gt(trailingTakeProfit)) {
          logger.info({ mint: poolKeys.baseMint.toString() }, `Trailing take profit triggered`);
          await this.sell(poolKeys.baseMint, await getAccount(this.connection, amountIn.token.mint));
          break;
        }

        await sleep(this.config.priceCheckInterval);
      } catch (e) {
        logger.trace({ mint: poolKeys.baseMint.toString(), e }, `Failed to check token price`);
      } finally {
        timesChecked++;
      }
    } while (timesChecked < timesToCheck);
  }
}