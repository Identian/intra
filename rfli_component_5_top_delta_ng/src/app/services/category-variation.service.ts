import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { CategoryDetailResponse, CategoryResponse } from '../models/models';
import { environment } from 'src/environments/environment';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root',
})
export class CategoryVariationService {

  constructor(private http: HttpClient) {}
  gradient = [
    'FF0000',
    'FF1100',
    'FF2200',
    'FF3300',
    'FF4400',
    'FF5500',
    'FF6600',
    'FF7700',
    'FF8800',
    'FF9900',
    'FFAA00',
    'FFBB00',
    'FFCC00',
    'FFDD00',
    'FFEE00',
    'FFFF00',
    'EEFF00',
    'DDFF00',
    'CCFF00',
    'BBFF00',
    'AAFF00',
    '99FF00',
    '88FF00',
    '77FF00',
    '66FF00',
    '55FF00',
    '44FF00',
    '33FF00',
    '22FF00',
    '11FF00',
    '00FF00',
  ];

  variationColors = {
    'positive': '246,53,56',    
    'neutral' : '65,69,84',   
    'negative' : '48,204,90'
  }

  public getCategories():Observable<CategoryResponse>{
    return this.http.get<CategoryResponse>(environment.category);
  }

  public getCategoryDetail(id:number):Observable<CategoryDetailResponse>{
    return this.http.get<CategoryDetailResponse>(`${environment.category_detail}?ranking_index=${id}`);
  }
  
}
