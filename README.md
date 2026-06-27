# Démo Kafka : suivre une donnée de bout en bout (CDC PostgreSQL → Kafka → PostgreSQL)

Ce projet est un **support pédagogique**. Objectif : voir une donnée naître dans
une base, voyager dans Kafka, et atterrir dans une autre base — en sachant
précisément **ce que fait chaque brique**. Il est conçu pour être montré à des
étudiants ou expliqué à l'oral.

---

## 1. Le concept en une image

```
┌──────────────┐   écrit    ┌───────────────┐  capture les   ┌──────────┐
│  generator   │  ───────▶  │ postgres-     │  changements   │ orders-  │
│ (script Py)  │  INSERT/   │ source        │  (CDC) ───────▶│ source   │
│              │  UPDATE    │ (base métier) │                │(Debezium)│
└──────────────┘            └───────────────┘                └────┬─────┘
                                                                   │ publie
                                                                   ▼
                                                          ┌─────────────────┐
                                                          │   KAFKA TOPIC   │
                                                          │ shop.public.    │
                                                          │ orders          │
                                                          └────────┬────────┘
                                                                   │ consomme
                                                                   ▼
┌───────────────┐   écrit    ┌──────────────┐                ┌──────────┐
│ postgres-     │  ◀───────  │ orders-sink  │  ◀──────────── │  (lit le │
│ target        │  UPSERT    │ (JDBC sink)  │                │  topic)  │
│ (base copie)  │            └──────────────┘                └──────────┘
└───────────────┘
```

**En une phrase :** chaque écriture dans la base source est captée, transformée
en message Kafka, puis rejouée dans la base cible — en quasi temps réel.

---

## 2. Les concepts Kafka, expliqués simplement

| Terme | Analogie | Dans cette démo |
|-------|----------|-----------------|
| **Broker Kafka** | Le bureau de poste central | Le service `kafka`. Il reçoit, stocke et distribue les messages. |
| **Topic** | Une file d'attente / un journal nommé | `shop.public.orders` : tous les changements de la table `orders` y atterrissent. |
| **Message (event)** | Une lettre | Un événement = « la commande 42 est passée à *shipped* ». |
| **Producer** | Celui qui poste une lettre | Le connecteur source `orders-source` écrit dans le topic. |
| **Consumer** | Celui qui relève le courrier | Le connecteur sink `orders-sink` lit le topic. |
| **Kafka Connect** | Le service qui branche Kafka sur le monde extérieur | Le service `connect`. Il héberge les connecteurs (source + sink) sans qu'on écrive de code. |
| **Connecteur source** | Une prise « entrée » | `orders-source` : lit Postgres → écrit dans Kafka. |
| **Connecteur sink** | Une prise « sortie » | `orders-sink` : lit Kafka → écrit dans Postgres. |
| **Offset** | Le marque-page | Position de lecture d'un consumer dans le topic. Permet de reprendre où on s'était arrêté. |
| **Zookeeper** | Le coordinateur (annuaire interne) | Service `zookeeper`. Gère la coordination du cluster Kafka. |

### Le mot-clé central : CDC (Change Data Capture)

On ne *demande pas* à la base « quelles sont les nouveautés ? » en boucle.
À la place, **Debezium lit le journal interne de PostgreSQL** (le *WAL*, write-ahead
log — le même que Postgres utilise pour sa réplication) et émet un événement
**à chaque** `INSERT`, `UPDATE` ou `DELETE`. C'est efficace, fidèle et sans
polling. C'est exactement ce que font les vrais pipelines de données en
production.

> Pour que ça marche, la base source tourne avec `wal_level=logical`
> (voir `docker-compose.yml`, ligne `command: ... wal_level=logical`). Sans ça,
> Postgres n'expose pas ses changements.

---

## 3. Qui fait quoi : chaque fichier décortiqué

| Fichier / dossier | Rôle | Points clés |
|-------------------|------|-------------|
| `docker-compose.yml` | Décrit **tous les services** et comment ils se lancent | C'est le chef d'orchestre. Définit les 7 conteneurs et leurs dépendances. |
| `init-source.sql` | Crée la table `orders` + 3 lignes de départ dans la base source | Exécuté **une seule fois** au tout premier démarrage de `postgres-source`. |
| `generator/generator.py` | Simule une « appli métier » qui écrit en continu | Toutes les 2-5 s : 40 % de chances d'`INSERT`, 60 % d'`UPDATE` aléatoire. C'est lui qui fait « bouger » les données. |
| `connect/Dockerfile` | Construit l'image Kafka Connect avec les bons plugins | Installe les connecteurs **Debezium** (source) et **JDBC** (sink). |
| `connectors/source-connector.json` | Config du connecteur **source** (Debezium) | Quelle base lire, quelle table, quel topic produire. |
| `connectors/sink-connector.json` | Config du connecteur **sink** (JDBC) | Quel topic lire, dans quelle table écrire, comment gérer les doublons. |
| `register-connectors.sh` | Envoie ces 2 configs à Kafka Connect via son API REST | À lancer **après** que la stack soit démarrée. |

### Détail des deux connecteurs (le cœur de la démo)

**`source-connector.json`** — la prise d'entrée :

```jsonc
"connector.class": "io.debezium.connector.postgresql.PostgresConnector", // moteur CDC
"database.hostname": "postgres-source",   // quelle base écouter
"table.include.list": "public.orders",    // quelle table suivre
"topic.prefix": "shop",                    // → topic nommé "shop.public.orders"
"plugin.name": "pgoutput",                 // mécanisme de réplication logique natif de Postgres
"slot.name": "debezium_orders_slot",       // "robinet" de réplication créé côté Postgres
"transforms": "unwrap",                    // simplifie le message Debezium...
"transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"
// ...pour ne garder que l'état FINAL de la ligne (sinon le message est très verbeux)
```

> **Le `unwrap` est important à expliquer :** par défaut, Debezium envoie un
> message riche (`before` / `after` / métadonnées). `ExtractNewRecordState`
> « déballe » ça pour ne garder que la ligne telle qu'elle est *après* le
> changement — beaucoup plus simple à consommer pour le sink.

**`sink-connector.json`** — la prise de sortie :

```jsonc
"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector", // écrit en SQL
"topics": "shop.public.orders",          // quel topic consommer
"table.name.format": "orders",           // dans quelle table écrire (côté target)
"insert.mode": "upsert",                 // INSERT si nouveau, UPDATE si existe déjà
"pk.mode": "record_key",                 // la clé Kafka sert de clé primaire...
"pk.fields": "id",                        // ...c'est la colonne "id"
"auto.create": "true",                   // crée la table cible automatiquement
"auto.evolve": "true"                     // ajoute des colonnes si le schéma change
```

> **L'`upsert` est la clé d'un pipeline correct :** quand la commande 42 passe
> de *pending* à *shipped*, on veut **mettre à jour** la ligne 42 dans la cible,
> pas en créer une nouvelle. `upsert` + clé primaire `id` garantissent ça.

---

## 4. Les 7 services (`docker compose ps`)

| Service | Rôle | Port exposé (hôte) |
|---------|------|--------------------|
| `zookeeper` | Coordination du cluster Kafka | — (interne) |
| `kafka` | Le broker : stocke et distribue les messages | `9092` |
| `postgres-source` | Base « métier » d'origine (`sourcedb`) | `5433` |
| `postgres-target` | Base « copie » d'arrivée (`targetdb`) | `5434` |
| `connect` | Kafka Connect : héberge les 2 connecteurs | `8083` (API REST) |
| `kafka-ui` | Interface web pour explorer Kafka | `8080` |
| `generator` | Script Python qui écrit en boucle dans la source | — |

> Note ports : la source est sur **5433** et la cible sur **5434** côté hôte
> (les deux écoutent 5432 *à l'intérieur* de leur conteneur). C'est pour pouvoir
> les distinguer depuis ta machine.

---

## ⚠️ Note Windows / PowerShell (à lire avant de copier-coller)

Les commandes ci-dessous sont **adaptées à Windows PowerShell**. Si tu suis un
tuto Linux/Mac, attention à 3 pièges classiques :

| Piège Linux/Bash | Pourquoi ça casse sous PowerShell | À utiliser à la place |
|------------------|-----------------------------------|------------------------|
| `curl -s ...` | `curl` est un **alias** de `Invoke-WebRequest`, pas le vrai curl | `curl.exe -s ...` **ou** `Invoke-RestMethod ...` |
| `\` en fin de ligne | Le retour à la ligne se fait avec un backtick `` ` ``, pas `\` | Écris la commande **sur une seule ligne** |
| `python3` | N'existe en général pas sous Windows | `python` |

Et pour les noms de conteneurs : ils sont **préfixés par le nom du dossier**
(ici `14_...`). Le plus simple est d'utiliser `docker compose exec <service>`,
qui ne dépend ni de l'OS ni du préfixe.

---

## 5. Prérequis

- **Docker Desktop installé ET démarré.** Si tu vois une erreur du type
  `open //./pipe/dockerDesktopLinuxEngine: The system cannot find the file specified`,
  c'est que Docker Desktop n'est pas lancé. Ouvre-le et attends que le moteur
  soit « running ».
- Quelques minutes : la première construction de l'image Kafka Connect télécharge
  les plugins Debezium et JDBC (1 à 3 min selon la connexion).

Vérifier que Docker répond :

```powershell
docker info --format '{{.ServerVersion}}'
```

---

## 6. Démarrer le pipeline

### Étape 1 — Lancer l'infrastructure

```powershell
docker compose up -d --build
```

Attends que tout soit prêt (surtout `connect`, qui attend que Kafka et les deux
Postgres soient *healthy*). Vérifie :

```powershell
docker compose ps
```

Tous les services doivent être `Up` (et `healthy` pour ceux qui ont un
healthcheck). Pour suivre le démarrage de Kafka Connect :

```powershell
docker compose logs -f connect
```

### Étape 2 — Enregistrer les connecteurs

C'est **l'étape qu'on oublie le plus souvent** : tant qu'on ne l'a pas faite,
aucune donnée ne circule (le topic reste vide, `/connectors` renvoie une liste
vide). On envoie les 2 configs JSON à l'API REST de Kafka Connect.

**Option A — via le script (Git Bash) :**

```bash
bash register-connectors.sh
```

**Option B — manuellement en PowerShell** (si tu n'as pas Bash) :

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8083/connectors `
  -ContentType "application/json" -InFile connectors/source-connector.json

Invoke-RestMethod -Method Post -Uri http://localhost:8083/connectors `
  -ContentType "application/json" -InFile connectors/sink-connector.json
```

### Étape 3 — Vérifier que les connecteurs tournent

Le champ `state` doit valoir `RUNNING`, **à la fois** pour le connecteur ET pour
sa tâche (`tasks`).

```powershell
Invoke-RestMethod http://localhost:8083/connectors/orders-source/status | ConvertTo-Json -Depth 10
Invoke-RestMethod http://localhost:8083/connectors/orders-sink/status   | ConvertTo-Json -Depth 10
```

Lister tous les connecteurs enregistrés :

```powershell
Invoke-RestMethod http://localhost:8083/connectors
```

---

## 7. Suivre la donnée de bout en bout (le cœur de la démo)

Voici le parcours complet d'un changement, étape par étape. **C'est ce qu'il faut
montrer à l'oral**, en ouvrant 3 fenêtres côte à côte.

### Maillon 1 — La donnée naît : la base SOURCE

Le `generator` écrit en continu. Regarde-le travailler :

```powershell
docker compose logs -f generator
```

Tu verras défiler des lignes comme :

```
INSERT  order 55 (Diana, 312.40 EUR)
UPDATE  order 42: pending -> shipped
```

Et l'état réel de la table source :

```powershell
docker compose exec postgres-source psql -U postgres -d sourcedb -c "SELECT * FROM orders ORDER BY id;"
```

### Maillon 2 — La donnée voyage : le TOPIC Kafka

Ouvre **Kafka UI** : http://localhost:8080

1. Menu **Topics** → clique sur `shop.public.orders`
2. Onglet **Messages**

Chaque `INSERT`/`UPDATE` côté source y apparaît comme un message JSON. Tu vois
la **clé** (l'`id` de la commande) et la **valeur** (l'état de la ligne). C'est
la preuve visuelle que la donnée transite par Kafka.

> L'onglet **Connect** de Kafka UI montre aussi l'état des 2 connecteurs, façon
> tableau de bord.

En ligne de commande, tu peux aussi lire le topic directement depuis le broker :

```powershell
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic shop.public.orders --from-beginning --max-messages 5
```

### Maillon 3 — La donnée atterrit : la base TARGET

Le résultat final du pipeline. Relance cette commande plusieurs fois : les
lignes évoluent en quasi temps réel, **en miroir** de la source.

```powershell
docker compose exec postgres-target psql -U postgres -d targetdb -c "SELECT * FROM orders ORDER BY id;"
```

### La démonstration qui convainc

Mets côte à côte la source et la cible, puis compare le **nombre de lignes** :

```powershell
docker compose exec postgres-source psql -U postgres -d sourcedb -c "SELECT count(*) FROM orders;"
docker compose exec postgres-target psql -U postgres -d targetdb -c "SELECT count(*) FROM orders;"
```

Les deux comptes se suivent. **La cible n'est jamais alimentée directement** :
tout passe par Kafka. C'est ça, le pipeline.

---

## 8. Expériences à faire (pour aller plus loin)

- **Deviens le générateur toi-même.** Connecte-toi à la source avec un client SQL
  (DBeaver, pgAdmin…) sur `localhost:5433` (user `postgres`, mdp `postgres`,
  base `sourcedb`) et fais un `INSERT`/`UPDATE` à la main. Regarde-le apparaître
  dans Kafka UI puis dans la cible. Sans Bash, en une ligne :

  ```powershell
  docker compose exec postgres-source psql -U postgres -d sourcedb -c "UPDATE orders SET status='delivered' WHERE id=1;"
  ```

  Puis vérifie côté cible que la ligne 1 est bien passée à `delivered`.

- **Coupe le générateur** et constate que le pipeline réagit pareil à tes
  modifications manuelles :

  ```powershell
  docker compose stop generator
  ```

- **Le cas des suppressions (`DELETE`).** Par défaut, la transformation
  `ExtractNewRecordState` **ignore** les suppressions (la ligne supprimée côté
  source reste dans la cible). Pour les propager, on ajouterait dans
  `source-connector.json` :
  `"transforms.unwrap.delete.handling.mode": "rewrite"` (ajoute une colonne
  `__deleted`), ou `"none"` combiné à `"delete.enabled": "true"` côté sink.
  Bon sujet de discussion : *« que veut dire supprimer une donnée dans un
  système événementiel ? »*

---

## 9. Dépannage rapide

| Symptôme | Cause probable | Solution |
|----------|----------------|----------|
| `open //./pipe/dockerDesktopLinuxEngine...` | Docker Desktop pas démarré | Lance Docker Desktop, attends « running » |
| `curl : Lecteur introuvable...` | `curl` = alias PowerShell | Utilise `curl.exe` ou `Invoke-RestMethod` |
| `404 Not Found` sur `/connectors/orders-source/status` | Connecteurs pas enregistrés | Refais l'**étape 2** |
| `No such container: kafka-streaming-demo-...` | Mauvais nom de conteneur | Utilise `docker compose exec <service> ...` |
| Le topic reste vide dans Kafka UI | Connecteur source pas `RUNNING` | Vérifie son `status`, regarde `docker compose logs connect` |
| La cible ne se met pas à jour | Connecteur sink en erreur | Vérifie le `status` du sink |

Voir le log d'un connecteur en erreur :

```powershell
docker compose logs --tail 50 connect
```

---

## 10. Tout arrêter / nettoyer

```powershell
docker compose down -v
```

Le `-v` supprime aussi les **volumes** : les données des deux bases ET les topics
Kafka sont effacés, pour repartir de zéro. (Sans `-v`, les données persistent et
au prochain démarrage `init-source.sql` ne sera **pas** rejoué, car la base
existe déjà.)
