import { CategoryDetailResponse, CategoryResponse, CategoryVariation, Folio } from './models/models';
import { CategoryVariationService } from './services/category-variation.service';
import { Component, ElementRef, OnInit, QueryList, Renderer2, ViewChild, ViewChildren, } from '@angular/core';
import { lastValueFrom } from 'rxjs';
import { NgxSpinnerService } from 'ngx-spinner';
import * as $ from 'jquery'
import * as XLSX from 'xlsx';

declare var window: any;

@Component({
  selector: 'top-delta',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss'],
})

export class AppComponent implements OnInit {
  title = 'top-delta';
  categoryVariation!: CategoryResponse;
  colors = this.categoryService.gradient;
  positiveColors = this.categoryService.gradient.slice(0, 15).reverse();
  negativeColors = this.categoryService.gradient.slice(16, 31);
  variationColors = this.categoryService.variationColors;
  informationModal !: any;
  selectedCategoryVariation !: CategoryVariation;
  selectedDetail!: CategoryDetailResponse;
  spinnerCategory = "spinnerCategory";
  spinnerDetail = "spinnerDetail";
  fechaString = new Date().toLocaleString();
  seeIndicator = false;
  mensajeTooltip = 'Diferencias indicadas en puntos básicos. Los campos en verde corresponden a valorizaciones; los campos en rojo corresponden a desvalorizaciones.'
  nextUpdate: number = 0;
  version: number = 0;
  nextStatus = '';
  page = 1;
  pageSize = 15;
  folios !: Folio[];
  currentlyVisibleFolios !: Folio[];
  @ViewChild('comp5FoliosTable') table!: ElementRef;

  @ViewChildren('variation', { read: ElementRef }) set variations(
    content: QueryList<any>
  ) {
    if (content) {
      content.forEach((item, index) => {
        let value = this.categoryVariation.data[index].tir_variation;
        let colorIndex = this.calculateColorIndex(value);

        let color = this.getVariationColor(value);
        if (value > 0) {
          color = `rgba(${this.variationColors.positive},${color})`
        } else if (value < 0) {
          color = `rgba(${this.variationColors.negative},${color})`;
        } else {
          color = `rgba(0,0,0,0)`;
        }

        this.renderer.setStyle(
          item.nativeElement,
          'background-color',
          color
        );
      });
    }
  }

  constructor(private renderer: Renderer2, private categoryService: CategoryVariationService,
    private spinner: NgxSpinnerService) {

  }

  showIndicator() {
    this.seeIndicator = true;
    this.fechaString = new Date().toLocaleString();
    setTimeout(() => {
      this.seeIndicator = false;
    }, 30000)
  }

  showSpinner(name: string) {
    this.spinner.show(name);

    setTimeout(() => {
      /** spinner ends after 5 seconds */
      this.spinner.hide(name);
    }, 5000);
  }

  async getCategories() {
    return await lastValueFrom(this.categoryService.getCategories());
  }

  reload(reloadTime?: number) {
    let timeToWait = (reloadTime != null ? reloadTime : this.nextUpdate) * 1000;

    setTimeout(() => {
      this.categoryService.getCategories().subscribe((response: CategoryResponse) => {
        if (response.version == this.version && response.next_status == 'intraday') {
          let relTime = Math.floor(Math.random() * 60);
          this.reload(relTime);
        } else {
          this.nextUpdate = response.next_update;
          this.version = response.version;
          this.nextStatus = response.next_status;

          if (response.data != null || response.data != undefined) {
            this.categoryVariation = response;
            this.showIndicator();
            if (this.nextUpdate > 0) {
              this.reload();
            } else {
              let relTime = Math.floor(Math.random() * 60);
              this.reload(relTime);
            }
          }
        }
      });
    }, timeToWait);

  }

  calculateColorIndex(value: number): number {
    let values = this.categoryVariation.data.map((item) => item.tir_variation);
    let max = Math.max(...values);
    let min = Math.min(...values);
    let index = 0;

    if (value > 0) {
      index = Math.round((value * 14) / max);
    } else if (value < 0) {
      index = Math.round((value * 14) / min);
    } else {
      index = 15;
    }
    return index;
  }

  getVariationColor(value: number): string {

    let values = this.categoryVariation.data.map((item) => item.tir_variation);
    let max = Math.max(...values);
    let min = Math.min(...values);
    let percentage = "";

    if (value > 0) {
      percentage = (value / max).toFixed(2);
    } else if (value < 0) {
      percentage = (value / min).toFixed(2);
    } else {
      percentage = "0";
    }


    return percentage;


  }

  median(numbers: number[]) {
    const sorted = Array.from(numbers).sort((a, b) => a - b);
    const middle = Math.floor(sorted.length / 2);
    if (sorted.length % 2 === 0) {
      return (sorted[middle - 1] + sorted[middle]) / 2;
    }
    return sorted[middle];
  }


  getCategoryDetail(id: number) {
    return this.categoryService.getCategoryDetail(id);
  }

  ngOnInit(): void {
    this.showIndicator();


    this.informationModal = new window.bootstrap.Modal(
      document.getElementById('topDeltaInformationModal')
    );

    document.addEventListener('hide.bs.modal', () => {
      $('#topDeltaInformationModal').appendTo('#topDeltaInformationModalContainer');
    });

    this.spinner.show(this.spinnerCategory);
    this.categoryService.getCategories().subscribe((response: CategoryResponse) => {
      this.categoryVariation = response;
      this.nextUpdate = response.next_update;
      this.version = response.version;
      this.nextStatus = response.next_status;
      this.spinner.hide(this.spinnerCategory);
      this.reload();
    })

  }


  async fetchCategoryInfo() {
    this.spinner.show(this.spinnerCategory);
    this.categoryService.getCategories().subscribe((response: CategoryResponse) => {
      this.categoryVariation = response;
      this.nextUpdate = response.next_update;
      this.version = response.version;
      this.nextStatus = response.next_status;
      this.spinner.hide(this.spinnerCategory);
    });
  }

  async openInformationModal(variation: CategoryVariation, index: number) {
    this.spinner.show(this.spinnerDetail);
    this.selectedCategoryVariation = variation;
    this.selectedDetail = await lastValueFrom(this.getCategoryDetail(index));
    if (this.selectedDetail.version != this.version) {
      this.spinner.hide(this.spinnerDetail);
      confirm('Hemos detectado que la información a la que está intentando acceder es diferente. Por favor, permítenos volver a cargar los datos. Te pedimos que vuelvas a seleccionar la categoria.')
      await this.fetchCategoryInfo()
    }
    else {
      this.folios = this.selectedDetail.data.folios;
      this.spinner.hide(this.spinnerDetail);
      this.refreshFolios();
      $('#topDeltaInformationModal').appendTo('#modalContainer');
      this.informationModal.show();
    }
  }

  refreshFolios() {
    this.currentlyVisibleFolios = this.folios.map((folio, i) => ({ id: i + 1, ...folio })).slice(
      (this.page - 1) * this.pageSize,
      (this.page - 1) * this.pageSize + this.pageSize,
    )
  }

  downloadFolios() {
    const ws: XLSX.WorkSheet = XLSX.utils.table_to_sheet(
      this.table.nativeElement
    );

    let date = new Date();
    /* new format */
    var fmt = '0.00';
    /* change cell format of range B2:D4 */
    var range = { s: { r: 1, c: 1 }, e: { r: 2, c: 100000 } };
    for (var R = range.s.r; R <= range.e.r; ++R) {
      for (var C = range.s.c; C <= range.e.c; ++C) {
        var cell = ws[XLSX.utils.encode_cell({ r: R, c: C })];
        if (!cell || cell.t != 'n') continue; // only format numeric cells
        cell.z = fmt;
      }
    }
    const wb: XLSX.WorkBook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'Hoja 1');
    var fmt = '@';
    wb.Sheets['Hoja 1']['F'] = fmt;

    /* save to file */
    XLSX.writeFile(
      wb,
      `Detalle_variacion${this.selectedCategoryVariation.category_id}_${date.toISOString().split('T')[0]}_${date
        .getHours()
        .toString()}${date.getMinutes().toString()}${date
          .getSeconds()
          .toString()}.xlsx`
    );
  }
}
