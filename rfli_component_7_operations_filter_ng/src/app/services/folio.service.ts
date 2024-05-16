import { environment } from 'src/environments/environment';
import { HttpClient } from '@angular/common/http'
import { IFolio } from '../interfaces/Folios';
import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class FolioService {

  constructor(private http: HttpClient) { }


  getFoliosIncluded(page: number, version: number) {
    return this.http.get<IFolio>(`${environment.api}/operations-filter?num-control=1&page=${page}&version=${version}`)
  }

  getFoliosExcluded(page: number, version: number) {
    return this.http.get<IFolio>(`${environment.api}/operations-filter?num-control=0&page=${page}&version=${version}`)
  }

}
