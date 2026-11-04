# Telegram Uploader

[![Python](https://img.shields.io/badge/Python-3.11%2B-blue)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-Hub-2496ED?logo=docker&logoColor=white)](https://hub.docker.com/)
[![Platforms](https://img.shields.io/badge/platforms-amd64%20%7C%20arm64-blue)](#arquitecturas-soportadas)

**Telegram Uploader** es una herramienta escrita en Python para sincronizar automáticamente una biblioteca local de series de televisión con un supergrupo de Telegram que tenga activados los temas (Topics).

La aplicación utiliza **un tema de Telegram por cada serie**, detecta qué archivos ya se han enviado y sube únicamente los que faltan. Está pensada para ejecutarse de forma continua en Docker, por ejemplo en un servidor Unraid.

> Este proyecto es una herramienta personal/comunitaria. Antes de utilizarlo en una biblioteca importante, pruébalo con una carpeta pequeña y una cuenta/grupo de pruebas. Respeta las condiciones de uso de Telegram y los derechos sobre los archivos que compartas.

## Índice

- [Características](#características)
- [Cómo funciona](#cómo-funciona)
- [Requisitos](#requisitos)
- [Estructura de carpetas](#estructura-de-carpetas)
- [Instalación desde Docker Hub](#instalación-desde-docker-hub)
- [Construcción local para desarrollo](#construcción-local-para-desarrollo)
- [Publicación de versiones](#publicación-de-versiones)
- [Arquitecturas soportadas](#arquitecturas-soportadas)
- [Configuración](#configuración)
- [Primer inicio y autenticación de Telegram](#primer-inicio-y-autenticación-de-telegram)
- [Programación de las sincronizaciones](#programación-de-las-sincronizaciones)
- [Estado persistente y archivos generados](#estado-persistente-y-archivos-generados)
- [Formatos reconocidos](#formatos-reconocidos)
- [Registros y solución de problemas](#registros-y-solución-de-problemas)
- [Seguridad](#seguridad)
- [Limitaciones conocidas](#limitaciones-conocidas)
- [Dependencias y agradecimientos](#dependencias-y-agradecimientos)
- [Licencia](#licencia)

## Características

- **Organización por temas:** crea un tema dentro del supergrupo de Telegram para cada carpeta de serie.
- **Sincronización incremental:** compara los archivos locales con los documentos existentes en cada tema y evita volver a subir episodios ya registrados.
- **Caché persistente:** guarda el estado de los temas y el cursor `top_message` en `topic_cache.json`. Si no hay mensajes nuevos en un tema, puede evitar volver a descargar y analizar todo su historial.
- **Detección de episodios:** reconoce patrones habituales como `S01E02`, `1x02`, fechas y numeración absoluta.
- **Subtítulos:** contempla formatos habituales de subtítulos y los envía como archivos independientes.
- **Empaquetado con 7-Zip:** puede empaquetar vídeos y dividir archivos grandes en volúmenes. Permite cifrar los archivos con contraseña.
- **Control de cuota diaria:** registra el volumen enviado y evita iniciar archivos o volúmenes que no quepan en la cuota restante.
- **Reintentos y FloodWait:** incorpora reintentos ante errores de red y respeta las esperas solicitadas por Telegram.
- **Control de progreso:** muestra el progreso de subida en los registros y en Telegram.
- **Panel de control opcional:** si se configura un bot, publica un botón para cancelar la jornada. La cancelación termina primero el archivo que esté en curso.
- **Integración opcional con TMDb:** puede añadir una cabecera con título, sinopsis, valoración y póster de la serie.
- **Informes:** envía un resumen de la sincronización, archivos pendientes y carpetas cuyos contenidos aparecen completos en Telegram.
- **Supervisor:** ejecuta cada ciclo de sincronización, espera hasta la siguiente ejecución y reintenta ante fallos recuperables.
- **Cierre ordenado:** atiende señales de parada para intentar finalizar de forma controlada el trabajo en curso.

## Cómo funciona

1. La aplicación examina las carpetas que cuelgan directamente de `MEDIA_DIR`. Cada carpeta se interpreta como una serie.
2. Recorre recursivamente cada carpeta y localiza vídeos y subtítulos admitidos.
3. Consulta los temas del supergrupo y relaciona cada tema con su carpeta.
4. Analiza el contenido remoto, utilizando la caché persistente cuando puede hacerlo.
5. Identifica los archivos que todavía no están completos en Telegram.
6. Espera a que los archivos tengan la antigüedad mínima configurada, para no intentar subir un archivo que todavía se está copiando.
7. Crea el tema si la serie aún no lo tiene, publica la cabecera y sube los archivos pendientes.
8. Actualiza la cuota, la caché y los informes.
9. El supervisor espera hasta la siguiente ejecución.

**Importante:** el informe de carpetas «liberables» es informativo. El programa no borra los vídeos ni los subtítulos originales de `MEDIA_DIR`.

## Requisitos

- Un servidor Linux o NAS con Docker Engine y Docker Compose.
- Python 3.11 o superior si se ejecuta fuera de Docker.
- Una cuenta de Telegram con credenciales de API (`api_id` y `api_hash`).
- Un supergrupo de Telegram con la función **Temas** activada.
- Permisos suficientes para que la cuenta de Telegram pueda consultar el grupo, crear temas y enviar mensajes/documentos.
- Espacio temporal suficiente para generar archivos 7z. El volumen temporal debe tener espacio libre para el archivo que se vaya a empaquetar.
- Conexión a Internet para acceder a Telegram y, si se habilita, a TMDb.

El contenedor instala 7-Zip y la biblioteca Hydrogram a partir de los archivos de este repositorio.

## Estructura de carpetas

Ejemplo de distribución en el host:

```text
/mnt/user/appdata/telegram-uploader/
├── data/
│   ├── user_session.*
│   ├── bot_session.*
│   ├── topic_cache.json
│   ├── cuota_hoy.json
│   ├── cuota_hoy.json.bak
│   └── liberables.txt
└── tmp/
    ├── archivos 7z temporales
    ├── pósteres temporales
    └── informes temporales
