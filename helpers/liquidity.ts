import { PublicKey } from '@solana/web3.js';
import { LiquidityPoolKeysV4, LiquidityStateV4, MAINNET_PROGRAM_ID } from '@raydium-io/raydium-sdk';
import { MinimalMarketLayoutV3 } from './market';
import BN from 'bn.js';

export function createPoolKeys(
  id: PublicKey,
  poolState: Partial<LiquidityStateV4>,
  marketState: MinimalMarketLayoutV3 | null
): LiquidityPoolKeysV4 {
  return {
    id,
    baseMint: poolState.baseMint || PublicKey.default,
    quoteMint: poolState.quoteMint || PublicKey.default,
    lpMint: poolState.lpMint || PublicKey.default,
    baseDecimals: (poolState.baseDecimal as BN | undefined)?.toNumber() || 0,
    quoteDecimals: (poolState.quoteDecimal as BN | undefined)?.toNumber() || 0,
    lpDecimals: (poolState.lpReserve as BN | undefined)?.toNumber() || 0,
    version: 4,
    programId: MAINNET_PROGRAM_ID.AmmV4,
    authority: PublicKey.findProgramAddressSync(
      [Buffer.from('amm authority')],
      MAINNET_PROGRAM_ID.AmmV4
    )[0],
    openOrders: poolState.openOrders || PublicKey.default,
    targetOrders: poolState.targetOrders || PublicKey.default,
    baseVault: poolState.baseVault || PublicKey.default,
    quoteVault: poolState.quoteVault || PublicKey.default,
    withdrawQueue: poolState.withdrawQueue || PublicKey.default,
    lpVault: poolState.lpVault || PublicKey.default,
    marketVersion: 3,
    marketProgramId: poolState.marketProgramId || PublicKey.default,
    marketId: poolState.marketId || PublicKey.default,
    marketAuthority: poolState.marketId ? PublicKey.findProgramAddressSync(
      [poolState.marketId.toBuffer()],
      poolState.marketProgramId || MAINNET_PROGRAM_ID.AmmV4
    )[0] : PublicKey.default,
    marketBaseVault: poolState.baseVault || PublicKey.default,
    marketQuoteVault: poolState.quoteVault || PublicKey.default,
    marketBids: marketState ? marketState.bids : PublicKey.default,
    marketAsks: marketState ? marketState.asks : PublicKey.default,
    marketEventQueue: marketState ? marketState.eventQueue : PublicKey.default,
    lookupTableAccount: PublicKey.default
  };
}