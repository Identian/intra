import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { environment } from 'src/environments/environment';
import { TitlesRequest } from 'src/models/models';

@Injectable({
  providedIn: 'root'
})
export class SearchService {

  constructor(private http: HttpClient) { }

  public getIsin(isin: string) {
    return this.http.get(`${environment.isin}?isin=${isin}`);
  }

  public getIssuers() {
    return this.http.get(environment.issuers);
  }

  public getTitles(titlesRequest: TitlesRequest) {
    return this.http.post(environment.titles, titlesRequest)
  }

  public saveIsines(isines: string[]) {
    let isines_ = {
      "isines": isines
    };
    return this.http.post(environment.userIsines, isines_);
  }

}
