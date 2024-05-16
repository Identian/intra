import {
  AfterViewInit,
  Component,
  ElementRef,
  EventEmitter,
  Input,
  OnChanges,
  Output,
  SimpleChanges,
  ViewChild,
} from '@angular/core';
import { NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';
import { Observable, OperatorFunction } from 'rxjs';
import { debounceTime, distinctUntilChanged, map } from 'rxjs/operators';
import { FormsModule } from '@angular/forms';
import { JsonPipe } from '@angular/common';

declare var window: any;

@Component({
  selector: 'app-ngbd-typeahead-basic',
  standalone: true,
  templateUrl: './ngbd-typeahead-basic.component.html',
  imports: [NgbTypeaheadModule, FormsModule, JsonPipe],
  styleUrls: ['./ngbd-typeahead-basic.component.scss'],
})
export class NgbdTypeaheadBasicComponent implements AfterViewInit {

  ngAfterViewInit(): void {
    window.addEventListener('empty-typeahead', (e:any) => {
      this.result="";
    })
  }

  @ViewChild('typeahead') elem!: ElementRef;

  @Output() salida = new EventEmitter<string>();
  result!: string;

  @Input() list!: string[];

  search: OperatorFunction<string, readonly string[]> = (
    text$: Observable<string>
  ) =>
    text$.pipe(
      debounceTime(200),
      distinctUntilChanged(),
      map((term) => {
        return term.length < 0
          ? []
          : this.list
              .filter((v) => v.toLowerCase().indexOf(term.toLowerCase()) > -1)
              .slice(0, 50);
      })
    );

  emitSelectedValue(selectedValue: any): void {
    console.log(selectedValue.item);
    this.salida.emit(selectedValue.item);
  }

  keydown(event: any) {
    console.log('keydown');
    if (this.result == '' || this.result == undefined) {
      console.log('campo vacio');
      this.salida.emit('');
    }
  }

  public openTypeahead(): void {
    // Dispatch event on input element that NgbTypeahead is bound to
    this.elem.nativeElement.dispatchEvent(new Event('input'));
    // Ensure input has focus so the user can start typing
    this.elem.nativeElement.focus();
  }
}
