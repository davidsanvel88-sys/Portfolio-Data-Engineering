
# Portfolio-Data-Engineering

# 🏭 ETL de Producción Industrial Automatizado

Este proyecto resuelve un problema común en plantas de manufactura: la consolidación de reportes de producción heterogéneos provenientes de diferentes sistemas legacy y operadores manuales.

## 🎯 El Problema
La empresa recibía diariamente 3 tipos de archivos con formatos inconsistentes:
*   **Formato A:** Archivos con valores nulos faltantes.
*   **Formato B:** Archivos con fechas no estandarizadas (DD/MM/YYYY) y texto sucio en campos numéricos ("100 pzas").
*   **Formato C:** Reportes legacy con filas de encabezado basura y delimitadores incorrectos.

## 🛠️ La Solución Técnica
Desarrollé un script en **Python (Pandas)** que:
1.  **Detecta automáticamente** el formato del archivo.
2.  **Limpia y Normaliza** los datos (fechas ISO 8601, conversión numérica, renombrado de columnas).
3.  **Unifica** todo en un `Reporte_Maestro.csv` listo para Business Intelligence.
4.  **Genera un Análisis de KPIs** automático en consola (Producción por Máquina y Operador).

## 📊 Resultados
*   **Reducción de tiempo:** De 2 horas manuales a <3 segundos de ejecución.
*   **Calidad de datos:** 100% de registros limpios y validados.

### Evidencia de Ejecución
![Reporte de Consola](Captura%20de%20pantalla%20.jpg)
![dashboard ](https://github.com/user-attachments/assets/e935e85c-3677-4c70-af43-4df3473ae751)
