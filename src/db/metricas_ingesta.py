# src/db/metricas_ingesta.py
import time
import threading
from dataclasses import dataclass, field
from typing import Optional

@dataclass
class MetricasIngesta:
    """
    Recolecta datos durante la ingesta para estimar costos de la histórica.
    Thread-safe — puede usarse desde múltiples coroutines.
    """
    _lock: threading.Lock = field(default_factory=threading.Lock)

    # Throughput
    actas_ok:       int = 0
    actas_error:    int = 0
    actas_reintento: int = 0

    # Tamaño de respuesta INPI (para estimar costo de proxy)
    bytes_html_total: int = 0
    muestras_html:    int = 0

    # Tiempos de respuesta INPI
    tiempo_total_inpi: float = 0.0

    # DB
    lotes_db:        int = 0
    tiempo_total_db: float = 0.0

    # Control de tiempo
    inicio: float = field(default_factory=time.time)

    # Respuestas HTTP del INPI (para detectar bloqueos)
    codigos_http: dict = field(default_factory=dict)

    def registrar_html(self, html: str, tiempo_seg: float, codigo_http: int = 200):
        with self._lock:
            self.actas_ok       += 1
            self.bytes_html_total += len(html.encode('utf-8'))
            self.muestras_html  += 1
            self.tiempo_total_inpi += tiempo_seg
            self.codigos_http[codigo_http] = self.codigos_http.get(codigo_http, 0) + 1

    def registrar_error(self, reintento: bool = False):
        with self._lock:
            if reintento:
                self.actas_reintento += 1
            else:
                self.actas_error += 1

    def registrar_lote_db(self, n_actas: int, tiempo_seg: float):
        with self._lock:
            self.lotes_db       += 1
            self.tiempo_total_db += tiempo_seg

    def resumen(self) -> dict:
        elapsed = time.time() - self.inicio
        total   = self.actas_ok + self.actas_error
        if total == 0:
            return {"error": "sin datos todavía"}

        kb_promedio  = (self.bytes_html_total / self.muestras_html / 1024) if self.muestras_html else 0
        throughput   = self.actas_ok / elapsed if elapsed > 0 else 0

        # Proyecciones para 4MM actas
        MM           = 4_000_000
        horas_4mm    = (MM / throughput / 3600) if throughput > 0 else None
        gb_proxy_4mm = (kb_promedio * MM / 1024 / 1024) if kb_promedio > 0 else None
        costo_proxy  = gb_proxy_4mm * 0.70 if gb_proxy_4mm else None  # $0.70/GB datacenter

        return {
            "── Tiempo corriendo":        f"{elapsed/60:.1f} min",
            "── Actas OK":                self.actas_ok,
            "── Actas error":             self.actas_error,
            "── Reintentos":              self.actas_reintento,
            "── Tasa de error":           f"{self.actas_error/total*100:.1f}%",
            "── Throughput real":         f"{throughput:.1f} actas/seg",
            "── Tiempo promedio INPI":    f"{self.tiempo_total_inpi/max(self.actas_ok,1)*1000:.0f} ms",
            "── Tamaño HTML promedio":    f"{kb_promedio:.1f} KB",
            "── Tiempo promedio DB":      f"{self.tiempo_total_db/max(self.lotes_db,1)*1000:.0f} ms/lote",
            "── Códigos HTTP INPI":       self.codigos_http,
            "── PROYECCIÓN 4MM — horas":  f"{horas_4mm:.1f}h" if horas_4mm else "sin datos",
            "── PROYECCIÓN 4MM — GB proxy": f"{gb_proxy_4mm:.0f} GB" if gb_proxy_4mm else "sin datos",
            "── PROYECCIÓN 4MM — costo proxy": f"${costo_proxy:.0f}" if costo_proxy else "sin datos",
        }

    def imprimir_resumen(self):
        print("\n" + "="*55)
        print("  MÉTRICAS DE INGESTA")
        print("="*55)
        for k, v in self.resumen().items():
            print(f"  {k}: {v}")
        print("="*55 + "\n")


# Instancia global — se importa desde worker y desde inpi_marcas
metricas = MetricasIngesta()