export interface TitlesRequest{
    issuer   : string | null;
    rating   : number | null;
    rate_type: number | null;
    currency : string | null;
    maturity_days: {
        min:   number | null;
        max:  number  | null;
    }
    yield: {
        min:   number | null;
        max:   number | null;
    },
    class_name: string | null;
}

export interface IsinFilterResult {
    currency_type?: string;
    isin:          string;
    nemo:          string;
    issuer_name?:   string;
    maturity_days: number;
    rate_type?:     string;
    real_rating?:   string;
    yield:         number;
    equivalent_margin?: number;
}

export interface Isin {
    issuer_name: string;
    accrued_interest: number;
    yield: number;
    real_rating: string;
    rate_type: string;
    clean_price: number;
    equivalent_margin: number;
    maturity_date: string;
    isin: string;
    maturity_days: number;
    issue_date: string;
    maturity_range: number;
    convexity: number;
    mean_price: number;
    nemo: string;
    currency_type: string;
    margin: number;
    duration: number;
    modified_duration: number;
}