# Repositorio test-GHAS
# python-dbx-mitdataprocesses 

Repositorio del proyecto python-dbx-mitdataprocesses 

Repositorio python

## Flujo automático de ramas: develop → pre-release-qa → release-qa → release

El flujo de integración y despliegue de este repositorio está completamente automatizado mediante GitHub Actions. El proceso es el siguiente:

### 1. **De develop a pre-release-qa:**
- **Trigger:** Push a la rama `develop`
- **Action:** `sync_develop_to_pre_release_qa.yml`
- **Proceso:**
  - Crea una rama temporal `parametrizacion-qa-{timestamp}`
  - Sincroniza cambios desde `develop` a `pre-release-qa`
  - Parametriza automáticamente archivos `.env`, `.properties` y `.yaml` con valores de QA
  - Hace merge automático a `pre-release-qa`
  - Elimina la rama temporal

### 2. **De pre-release-qa a temp-test-qa:**
- **Trigger:** Pull Request cerrado (merged) desde `pre-release-qa` hacia `release-qa`
- **Action:** `sync_qa_to_release.yml`
- **Proceso:**
  - Crea una rama temporal `parametrizacion-release-{timestamp}`
  - Parametriza automáticamente archivos con valores de producción
  - Hace merge automático a `release`
  - Elimina la rama temporal

### Gráfico del flujo

```text
[develop]
    │
    │  (push automático)
    ▼
[pre-release-qa] ← Parametrización automática (QA)
    │
    │  (Pull Request merged)
    ▼
[release-qa]
    │
    │  (acción automática tras merge del PR)
    ▼
[release] ← Parametrización automática (Producción)
    │
    │  (Pull Request manual)
    ▼
[main / prod]
```
