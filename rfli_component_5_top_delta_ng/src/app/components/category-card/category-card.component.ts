import { Component, Input, ViewChild } from '@angular/core';
import { CategoryVariation } from 'src/app/models/models';

@Component({
  selector: 'precia-category-card',
  templateUrl: './category-card.component.html',
  styleUrls: ['./category-card.component.scss']
})
export class CategoryCardComponent {
  @Input() categoryVariation!: CategoryVariation;
}
