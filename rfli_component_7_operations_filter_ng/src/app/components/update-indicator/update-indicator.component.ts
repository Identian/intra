import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';

@Component({
  selector: 'app-update-indicator',
  standalone: true,
  imports: [CommonModule],
  template: `
    <div style="display: flex; flex-direction: row; align-items:center;">
      <svg height="30" width="40" *ngIf="!blinking && version != 1">
        <circle cx="20" cy="15" r="10" fill="#005e00" />
        Tu navegador no soporta SVG. Actualiza a uno más reciente.
      </svg>

      <svg
        height="30"
        width="40"
        class="blinking"
        *ngIf="blinking && version != 1"
      >
        <circle cx="20" cy="15" r="10" fill="#00ff00" />
        Tu navegador no soporta SVG. Actualiza a uno más reciente.
      </svg>

      <svg height="30" width="40" class="blinking" *ngIf="version == 1">
        <circle cx="20" cy="15" r="10" fill="red" />
        Tu navegador no soporta SVG. Actualiza a uno más reciente.
      </svg>

      <span
        ><i>Actualizado {{ date }} {{version==1? mensaje:''}}</i></span
      >

    </div>
  `,
  styleUrls: ['./update-indicator.component.css'],
})
export class UpdateIndicatorComponent {
  @Input() version!: number;
  mensaje = 'La información es del día anterior. Aún no hay información intradía';
  @Input() date: string = '';
  @Input() blinking = false;
}
