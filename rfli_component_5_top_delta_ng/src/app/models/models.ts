export interface CategoryVariation {
    category_id: number;
    abs_tir_variation: number;
    tir_variation: number;
    description: string;
    ranking_index: number;
    class_name: string;
    category_type: string;
}

export interface CategoryDetail {
    category_id: number;
    cc_curve: string;
    maturity_range: string;
    pbs_change: number;
    total_isines: number;
    folios: Folio[];
    short_isin_ref: IsinRef;
    medium_isin_ref: IsinRef;
    long_isin_ref: IsinRef;
}

export interface IsinRef {
    isin_code: string;
    instrument: string;
    maturity_days: string;
    issuer: string;
    today_yield: number;
    yesterday_yield: number;
    variation: number;
}

export interface Folio {
    amount: number;
    maturity_days: number;
    timestamp_operation: string;
    yield: number;
    maturity_date: string;
    nemo: string;
    sheet: number;
    folio_type?: string;
    trading_system?: string;
}

export interface BaseResponse {
    version: number;
    next_update: number;
    next_status: string;
}

export interface CategoryResponse extends BaseResponse {
    data: CategoryVariation[];
}

export interface CategoryDetailResponse extends BaseResponse {
    data: CategoryDetail;
}