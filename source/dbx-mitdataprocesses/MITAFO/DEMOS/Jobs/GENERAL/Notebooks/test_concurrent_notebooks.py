# Test para simular cientos de notebooks usando DBXConnectionManager
# Demuestra el patrón Singleton optimizado para concurrencia masiva

import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed


def simulate_notebook_execution(notebook_id: int):
    """
    Simula la ejecución de un notebook que usa DBXConnectionManager
    """
    try:
        start_time = time.time()

        # ⚡ NUEVO PATRÓN RECOMENDADO PARA CIENTOS DE NOTEBOOKS:
        from modules.dbxmanager.dbx_connection_manager import DBXConnectionManager

        # OPCIÓN 1: Constructor normal (ahora optimizado con Singleton)
        db = DBXConnectionManager()

        # OPCIÓN 2: Método explícito get_instance() - MÁS CLARO
        # db = DBXConnectionManager.get_instance()

        init_time = time.time() - start_time

        # Simular una operación write_delta pequeña
        from pyspark.sql import Row

        test_data = [
            Row(notebook_id=notebook_id, timestamp=time.time(), test="concurrent")
        ]
        test_df = db.spark.createDataFrame(test_data)

        # Tabla única por notebook para evitar conflictos
        table_name = f"test_concurrent_nb_{notebook_id}_{uuid.uuid4().hex[:6]}"

        write_start = time.time()
        db.write_delta(
            delta_name=table_name,
            dataframe=test_df,
            method="overwrite",
            fast_write_mode=True,
        )
        write_time = time.time() - write_start

        # Leer para verificar
        read_start = time.time()
        result_count = db.read_delta(table_name).count()
        read_time = time.time() - read_start

        # Limpiar
        db.drop_delta(table_name)

        total_time = time.time() - start_time

        return {
            "notebook_id": notebook_id,
            "success": True,
            "init_time": init_time,
            "write_time": write_time,
            "read_time": read_time,
            "total_time": total_time,
            "records": result_count,
        }

    except Exception as e:
        return {
            "notebook_id": notebook_id,
            "success": False,
            "error": str(e),
            "init_time": init_time if "init_time" in locals() else 0,
            "total_time": time.time() - start_time,
        }


def test_concurrent_notebooks(num_notebooks: int = 50, max_workers: int = 20):
    """
    Simula múltiples notebooks ejecutándose concurrentemente

    Args:
        num_notebooks: Número de notebooks a simular (50-200 recomendado)
        max_workers: Número máximo de threads concurrentes
    """

    print("🚀 SIMULANDO CIENTOS DE NOTEBOOKS CONCURRENTES")
    print("=" * 60)
    print(f"📊 Notebooks simulados: {num_notebooks}")
    print(f"🔧 Workers concurrentes: {max_workers}")
    print("=" * 60)

    start_time = time.time()
    results = []

    # Ejecutar notebooks concurrentemente
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Enviar todas las tareas
        future_to_notebook = {
            executor.submit(simulate_notebook_execution, notebook_id): notebook_id
            for notebook_id in range(1, num_notebooks + 1)
        }

        completed_count = 0
        for future in as_completed(future_to_notebook):
            result = future.result()
            results.append(result)
            completed_count += 1

            if completed_count % 10 == 0 or completed_count == num_notebooks:
                elapsed = time.time() - start_time
                print(
                    f"📈 Progreso: {completed_count}/{num_notebooks} ({completed_count/num_notebooks*100:.1f}%) - {elapsed:.1f}s"
                )

    total_time = time.time() - start_time

    # Analizar resultados
    successful = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]

    print("\n" + "=" * 60)
    print("📊 ANÁLISIS DE RESULTADOS")
    print("=" * 60)

    print(
        f"✅ Notebooks exitosos: {len(successful)}/{num_notebooks} ({len(successful)/num_notebooks*100:.1f}%)"
    )
    print(
        f"❌ Notebooks fallidos: {len(failed)}/{num_notebooks} ({len(failed)/num_notebooks*100:.1f}%)"
    )
    print(f"⏱️ Tiempo total: {total_time:.2f} segundos")

    if successful:
        init_times = [r["init_time"] for r in successful]
        write_times = [r["write_time"] for r in successful]
        read_times = [r["read_time"] for r in successful]
        total_times = [r["total_time"] for r in successful]

        print(f"\n📈 TIEMPOS PROMEDIO:")
        print(f"   🏗️ Inicialización: {sum(init_times)/len(init_times):.3f}s")
        print(f"   ✍️ Escritura Delta: {sum(write_times)/len(write_times):.3f}s")
        print(f"   📖 Lectura Delta: {sum(read_times)/len(read_times):.3f}s")
        print(f"   🔄 Total por notebook: {sum(total_times)/len(total_times):.3f}s")

        print(f"\n📊 RENDIMIENTO:")
        throughput = num_notebooks / total_time
        print(f"   🚀 Throughput: {throughput:.1f} notebooks/segundo")
        print(f"   ⚡ Operaciones Delta: {len(successful) * 2} (write + read)")
        print(f"   📈 Ops/segundo: {(len(successful) * 2) / total_time:.1f}")

    if failed:
        print(f"\n❌ ERRORES ENCONTRADOS:")
        error_counts = {}
        for fail in failed:
            error = fail.get("error", "Unknown")[:100]
            error_counts[error] = error_counts.get(error, 0) + 1

        for error, count in error_counts.items():
            print(f"   • {error}: {count} notebooks")

    # Análisis del patrón Singleton
    first_init = min([r["init_time"] for r in successful]) if successful else 0
    avg_init = (
        sum([r["init_time"] for r in successful]) / len(successful) if successful else 0
    )

    print(f"\n🏗️ ANÁLISIS SINGLETON:")
    if avg_init < 0.01:  # Menos de 10ms promedio
        print("   ✅ EXCELENTE: Singleton funcionando - inicializaciones ultra-rápidas")
        print(f"   ✅ Primera inicialización: {first_init:.3f}s")
        print(f"   ✅ Promedio reutilización: {avg_init:.3f}s")
    else:
        print("   ⚠️ Posible problema: Inicializaciones tomando tiempo")
        print(f"   ⚠️ Promedio inicialización: {avg_init:.3f}s")

    return {
        "total_notebooks": num_notebooks,
        "successful": len(successful),
        "failed": len(failed),
        "total_time": total_time,
        "throughput": len(successful) / total_time if total_time > 0 else 0,
        "avg_init_time": avg_init,
        "avg_write_time": (
            sum([r["write_time"] for r in successful]) / len(successful)
            if successful
            else 0
        ),
        "success_rate": (
            len(successful) / num_notebooks * 100 if num_notebooks > 0 else 0
        ),
    }


def quick_singleton_test():
    """
    Prueba rápida del patrón Singleton
    """
    print("🧪 PRUEBA RÁPIDA DEL PATRÓN SINGLETON")
    print("=" * 50)

    from modules.dbxmanager.dbx_connection_manager import DBXConnectionManager

    # Crear múltiples instancias
    print("🏗️ Creando 5 instancias...")

    instances = []
    for i in range(5):
        start = time.time()
        db = DBXConnectionManager()
        init_time = time.time() - start
        instances.append((db, init_time))
        print(f"   Instancia {i+1}: {init_time:.4f}s - ID: {id(db)}")

    # Verificar que todas son la misma instancia
    all_same = all(id(inst[0]) == id(instances[0][0]) for inst in instances)

    print(f"\n🔍 RESULTADO:")
    if all_same:
        print(
            "   ✅ PERFECTO: Todas las instancias son la misma (Singleton funcionando)"
        )
        print(f"   ✅ Primera inicialización: {instances[0][1]:.4f}s")
        avg_reuse = sum(inst[1] for inst in instances[1:]) / 4
        print(f"   ✅ Promedio reutilización: {avg_reuse:.4f}s")
    else:
        print("   ❌ ERROR: Se crearon múltiples instancias (Singleton no funciona)")

    return all_same


if __name__ == "__main__":
    # Prueba rápida del Singleton
    singleton_works = quick_singleton_test()

    if singleton_works:
        print("\n" + "=" * 60)
        # Simular notebooks concurrentes (empezar con pocos)
        test_concurrent_notebooks(num_notebooks=20, max_workers=10)
    else:
        print(
            "\n❌ Singleton no funciona correctamente - no ejecutar prueba concurrente"
        )
