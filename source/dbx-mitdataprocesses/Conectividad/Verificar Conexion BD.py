# Databricks notebook source
import socket
import logging

# Configurar logging (si no se ha configurado en otro lado)
logging.basicConfig(
    filename='conexion_bd.log',
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def verificar_conexion(host, puerto):
    try:
        with socket.create_connection((host, puerto), timeout=5):
            return True
    except (socket.timeout, socket.error) as e:
        # Log interno seguro
        logging.warning(f"No se pudo conectar a {host}:{puerto} - Detalle: {e}")
        return False


def verificar_varios_hosts(hosts_puertos):
    for host, puerto in hosts_puertos:
        print(f"Verificando conexión a {host}:{puerto}...")
        if verificar_conexion(host, puerto):
            print(f"¡Conexión exitosa a {host}:{puerto}!")
        else:
            print(f"Conexión fallida a {host}:{puerto}.\n")


# Lista de tuplas (host, puerto) para verificar
hosts_puertos = [
    ("10.11.90.105", 1521),
    ("10.11.90.95", 1521),
    ("10.11.90.49", 1521),
    ("10.11.90.76", 1521),
    ("10.11.90.177", 1521),
]

verificar_varios_hosts(hosts_puertos)
