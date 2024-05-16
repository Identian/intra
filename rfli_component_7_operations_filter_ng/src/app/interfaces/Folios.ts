export interface IFolio {
    version: number;
    next_update: number;
    next_status: string;
    total_size: number;
    page_size: number;
    data: FolioBase[];
}

export interface FolioBase {
    folio: number;
    nemo: string;
    maturity_days: number;
    amount: number;
    spread: number;
    category_id: string;
    cc_curve: string;
    trading_system: string;
    reason: string
    is_in_curve: string
}