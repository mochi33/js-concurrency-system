// ── Symbol metadata ──

export interface SymbolInfo {
  symbol: string;
  name: string;
  type: "crypto" | "stock";
  basePrice: number;
  /** Annual volatility (used for geometric Brownian motion) */
  volatility: number;
  /** Typical daily volume */
  typicalVolume: number;
}

/** Default symbols: 3 crypto + 3 stock */
export const DEFAULT_SYMBOLS: SymbolInfo[] = [
  {
    symbol: "BTC",
    name: "Bitcoin",
    type: "crypto",
    basePrice: 100_000,
    volatility: 0.6,
    typicalVolume: 25_000,
  },
  {
    symbol: "ETH",
    name: "Ethereum",
    type: "crypto",
    basePrice: 3_500,
    volatility: 0.7,
    typicalVolume: 500_000,
  },
  {
    symbol: "SOL",
    name: "Solana",
    type: "crypto",
    basePrice: 180,
    volatility: 0.9,
    typicalVolume: 10_000_000,
  },
  {
    symbol: "AAPL",
    name: "Apple Inc.",
    type: "stock",
    basePrice: 230,
    volatility: 0.25,
    typicalVolume: 50_000_000,
  },
  {
    symbol: "TSLA",
    name: "Tesla Inc.",
    type: "stock",
    basePrice: 350,
    volatility: 0.5,
    typicalVolume: 80_000_000,
  },
  {
    symbol: "NVDA",
    name: "NVIDIA Corp.",
    type: "stock",
    basePrice: 130,
    volatility: 0.45,
    typicalVolume: 40_000_000,
  },
];

/** Look up symbol info */
export function getSymbolInfo(symbol: string): SymbolInfo | undefined {
  return DEFAULT_SYMBOLS.find((s) => s.symbol === symbol);
}
