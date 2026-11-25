"""
producer.py - Générateur de passages aux urgences UrgenceNet.ma (projet Big Data 2).

Deux modes :

  1) Fichier (historique pour les Parties 2 et 3) :
       python producer.py --mode file --n 50000
     -> écrit urgences_history.jsonl (un événement JSON par ligne)

  2) Kafka (flux temps réel pour les Parties 4 et 5) :
       python producer.py --mode kafka --rate 20
     -> publie environ 20 événements par seconde dans le topic "urgences"
       (clé du message = visit_id, valeur = événement JSON en UTF-8)
       Ajoutez --dry-run pour afficher les événements sans Kafka.
"""

import argparse
import heapq
import json
import os
import random
import signal
import sys
import time
from collections import Counter
from datetime import datetime, timezone

# --------------------------------------------------------------------------
# Paramètres de la simulation
# --------------------------------------------------------------------------
CITY_SHARE = {"Casablanca": 0.38, "Rabat": 0.22, "Marrakech": 0.23, "Tanger": 0.17}
CITY_CODE = {"Casablanca": "CAS", "Rabat": "RAB", "Marrakech": "MAR", "Tanger": "TAN"}
N_PATIENTS = 14000          # pseudonymes tirés parmi P-00001 ... P-19999
P_OTHER_CITY = 0.04         # passage dans une autre ville que celle du patient

DEPARTMENTS = ["Médecine générale", "Traumatologie", "Pédiatrie", "Cardiologie", "Pneumologie",
               "Gynécologie-Obstétrique", "Chirurgie viscérale", "Neurologie"]
DEPT_MIX = [0.30, 0.22, 0.15, 0.09, 0.08, 0.07, 0.05, 0.04]
SPECIALTY = {"Cardiologie", "Pneumologie", "Chirurgie viscérale", "Neurologie"}

TYPES = ["CHU", "HOPITAL_REGIONAL", "CLINIQUE", "CENTRE_SANTE"]
ATTRACT = {"CHU": 3.5, "HOPITAL_REGIONAL": 2.2, "CLINIQUE": 1.0, "CENTRE_SANTE": 1.1}
N_DESKS = {"CHU": 4, "HOPITAL_REGIONAL": 3, "CLINIQUE": 2, "CENTRE_SANTE": 1}
# répartition des priorités 1..5 selon le type d'établissement
PRIO_MIX = {"CHU": [0.06, 0.17, 0.37, 0.28, 0.12],
            "HOPITAL_REGIONAL": [0.04, 0.14, 0.37, 0.32, 0.13],
            "CLINIQUE": [0.02, 0.10, 0.36, 0.36, 0.16],
            "CENTRE_SANTE": [0.01, 0.06, 0.30, 0.40, 0.23]}
# délai d'attente avant prise en charge : (médiane en minutes, dispersion log-normale) par priorité
WAIT = {1: (4, 0.4), 2: (12, 0.5), 3: (28, 0.5), 4: (45, 0.55), 5: (60, 0.6)}
WAIT_FACTOR = {"CHU": 1.35, "HOPITAL_REGIONAL": 1.15, "CLINIQUE": 0.55, "CENTRE_SANTE": 0.85}
MAX_WAIT_MIN = 300

P_TRANSFER = {"CHU": 0.012, "HOPITAL_REGIONAL": 0.045, "CLINIQUE": 0.05, "CENTRE_SANTE": 0.10}
PRIO_TRANSFER = {1: 3.0, 2: 2.0, 3: 1.0, 4: 0.45, 5: 0.2}
P_CROSS_CHU = 0.06          # transfert vers le CHU d'un autre groupe (autorisé)
P_RETURN = 0.10             # retour vers l'établissement d'origine après avis au CHU
P_LWBS = 0.05               # priorité 4-5 : départ sans prise en charge après le triage
P_WAIT_TYPO = 0.01          # attente >= 60 min saisie à la main en « hmm » (1 h 15 -> 115)
P_LATE = 0.05               # événement en retard
MAX_LATE_S = 15 * 60        # retard maximal : 15 minutes
P_DUP = 0.02                # doublon

# Volume d'arrivées selon l'heure de event_time (UTC ; heure locale = UTC + 1)
HOURLY = [0.45, 0.35, 0.28, 0.25, 0.25, 0.30, 0.45, 0.70, 1.00, 1.30, 1.45, 1.50,
          1.40, 1.30, 1.25, 1.25, 1.30, 1.40, 1.55, 1.60, 1.45, 1.15, 0.85, 0.60]
MEAN_HOURLY = sum(HOURLY) / 24

# Comportements particuliers : un épisode environ tous les N nouveaux passages
SCN_EVERY = {"a": 3000, "c": 1300, "d": 1500, "e": 1000, "self": 4000, "unknown": 3000}
P_SCN_B = 0.35              # part des transferts du groupe redirigés (voir _destination)


def iso(ts):
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Picker:
    """Tirage pondéré rapide (poids cumulés précalculés)."""

    def __init__(self, items, weights):
        self.items = list(items)
        acc, self.cum = 0.0, []
        for w in weights:
            acc += w
            self.cum.append(acc)

    def pick(self, rng):
        return rng.choices(self.items, cum_weights=self.cum)[0]


# --------------------------------------------------------------------------
# Population : établissements, postes de triage, services, patients
# --------------------------------------------------------------------------
class Population:
    def __init__(self, facilities, seed):
        rng = random.Random(seed)
        srng = random.Random(f"urgencenet/{seed}")
        self.fac = {f["facility_id"]: f for f in facilities}
        self.ids = [f["facility_id"] for f in facilities]
        self.cities = [c for c in CITY_SHARE if any(f["city"] == c for f in facilities)]
        self.groups = sorted({f["hospital_group"] for f in facilities})
        self.by_city = {c: [i for i in self.ids if self.fac[i]["city"] == c] for c in self.cities}
        self.by_group = {g: [i for i in self.ids if self.fac[i]["hospital_group"] == g] for g in self.groups}
        self.chu = {g: [i for i in self.by_group[g] if self.fac[i]["type"] == "CHU"] for g in self.groups}

        # postes de triage, numérotés par ville
        self.desks = {}
        n_city = Counter()
        for i in self.ids:
            f = self.fac[i]
            ds = []
            for _ in range(N_DESKS.get(f["type"], 1)):
                n_city[f["city"]] += 1
                ds.append(f"T-{CITY_CODE.get(f['city'], f['city'][:3].upper())}-{n_city[f['city']]:02d}")
            self.desks[i] = Picker(ds, [1.0 / (k + 1) ** 0.5 for k in range(len(ds))])

        # services disponibles
        self.depts = {}
        for i in self.ids:
            t = self.fac[i]["type"]
            if t == "CHU":
                ds = list(DEPARTMENTS)
            elif t == "HOPITAL_REGIONAL":
                ds = DEPARTMENTS[:7]
            elif t == "CLINIQUE":
                ds = ["Médecine générale", "Traumatologie", "Cardiologie"]
                ds += rng.sample(["Pédiatrie", "Gynécologie-Obstétrique", "Chirurgie viscérale", "Pneumologie"],
                                 rng.randint(1, 2))
            else:
                ds = ["Médecine générale", "Traumatologie", "Pédiatrie"]
            ds = [d for d in DEPARTMENTS if d in ds]
            self.depts[i] = Picker(ds, [DEPT_MIX[DEPARTMENTS.index(d)] for d in ds])
        self.dept_set = {i: set(self.depts[i].items) for i in self.ids}

        # attractivité des établissements
        attract = {i: ATTRACT.get(self.fac[i]["type"], 1.0) * rng.lognormvariate(0.0, 0.35) for i in self.ids}
        self.fac_pick = {c: Picker(self.by_city[c], [attract[i] for i in self.by_city[c]]) for c in self.cities}

        # patients
        self.patients = [f"P-{n:05d}" for n in rng.sample(range(1, 20000), N_PATIENTS)]
        city_pick = Picker(self.cities, [CITY_SHARE[c] for c in self.cities])
        self.home = {p: city_pick.pick(rng) for p in self.patients}
        activity = {p: min(2.5, rng.lognormvariate(0.0, 0.8)) for p in self.patients}

        # comportements particuliers
        pool = list(self.patients)
        srng.shuffle(pool)
        self.grp_a = pool[:2]
        self.grp_e = pool[2]
        for p in self.grp_a + [self.grp_e]:
            activity[p] = 0.0
        self.e_facs = srng.sample([i for i in self.by_city[self.home[self.grp_e]]
                                   if self.fac[i]["type"] != "CHU"], 4)

        non_chu = [i for i in self.ids if self.fac[i]["type"] != "CHU"]
        clinics = [i for i in non_chu if self.fac[i]["type"] == "CLINIQUE"]
        self.grp_b = srng.choice(clinics)
        b_group = self.fac[self.grp_b]["hospital_group"]
        pairs = [(x, y) for x in non_chu for y in non_chu
                 if x != y and self.fac[x]["city"] == self.fac[y]["city"]
                 and self.fac[x]["type"] in ("HOPITAL_REGIONAL", "CLINIQUE")
                 and self.fac[y]["type"] in ("HOPITAL_REGIONAL", "CLINIQUE")
                 and self.grp_b not in (x, y)]
        self.grp_c = srng.choice(pairs)
        senders = [i for i in non_chu if i not in self.grp_c and i != self.grp_b
                   and self.fac[i]["type"] != "CENTRE_SANTE"]
        s = srng.choice(senders)
        receivers = [i for i in non_chu if i not in self.grp_c and i != self.grp_b
                     and self.fac[i]["hospital_group"] != self.fac[s]["hospital_group"]
                     and self.fac[i]["hospital_group"] != b_group
                     and self.fac[i]["type"] in ("CLINIQUE", "HOPITAL_REGIONAL")]
        self.grp_d = (s, srng.choice(receivers))

        normal = [p for p in self.patients if activity[p] > 0]
        self.pat_pick = {c: Picker([p for p in normal if self.home[p] == c],
                                   [activity[p] for p in normal if self.home[p] == c]) for c in self.cities}
        self.city_pick = city_pick


# --------------------------------------------------------------------------
# Simulateur (événements discrets)
# --------------------------------------------------------------------------
class Simulator:
    def __init__(self, pop, rng, start_ts, visits_per_s, use_profile, visit_counter=1):
        self.pop = pop
        self.rng = rng
        self.t = float(start_ts)
        self.visits_per_s = visits_per_s
        self.use_profile = use_profile
        self.lam_max = visits_per_s * (max(HOURLY) / MEAN_HOURLY if use_profile else 1.0)
        self.visit_no = visit_counter
        self.heap = []
        self.seq = 0
        self.stats = Counter()
        self.a_count = 0
        self.n_new = 0
        self.busy = {}                           # patient -> fin de son passage en cours
        self.next_scn = {k: v * rng.uniform(0.2, 0.8) for k, v in SCN_EVERY.items()}
        self._push(self.t + rng.expovariate(self.lam_max), "NEW", None)

    def _push(self, ts, kind, payload):
        self.seq += 1
        heapq.heappush(self.heap, (ts, self.seq, kind, payload))

    def _load(self, ts):
        h = datetime.fromtimestamp(ts, tz=timezone.utc).hour
        return 0.6 + 0.4 * HOURLY[h] / MEAN_HOURLY

    # ---- nouveaux passages ----------------------------------------------
    def _new_visit(self, t):
        rng, pop = self.rng, self.pop
        self.n_new += 1
        due = [k for k in sorted(self.next_scn) if self.n_new >= self.next_scn[k]]
        scn = due[0] if due else None
        if scn:
            self.next_scn[scn] += SCN_EVERY[scn] * rng.uniform(0.8, 1.2)
        if scn == "a":
            self._scn_a(t)
            return
        if scn == "c":
            x, y = pop.grp_c
            plan = [y, x] + ([y] if rng.random() < 0.35 else [])
            patient = self._free_patient(t, pop.fac[x]["city"])
            dept = self._common_dept(x, y)
            self._visit(t, patient, x, dept, rng.choice([2, 3]), {"plan": plan, "short": True})
            return
        if scn == "d":
            s, d = pop.grp_d
            patient = self._free_patient(t, pop.fac[s]["city"])
            dept = self._common_dept(s, d)
            self._visit(t, patient, s, dept, rng.choice([2, 3, 3]), {"plan": [d]})
            return
        if scn == "e":
            f = rng.choice(pop.e_facs)
            self._visit(t, pop.grp_e, f, "Médecine générale", rng.choice([4, 4, 5]), {"discharge": True})
            return
        city = pop.city_pick.pick(rng)
        patient = self._free_patient(t, city)
        if rng.random() < P_OTHER_CITY:
            city = rng.choice(pop.cities)
        f = pop.fac_pick[city].pick(rng)
        # "self" : transfert vers soi-même ; "unknown" : vers un établissement absent du référentiel
        ctx = {"error": scn} if scn in ("self", "unknown") else {}
        self._visit(t, patient, f, pop.depts[f].pick(rng), None, ctx)

    def _free_patient(self, t, city):
        """Tire un patient de la ville qui n'est pas déjà aux urgences."""
        for _ in range(20):
            p = self.pop.pat_pick[city].pick(self.rng)
            if self.busy.get(p, 0) < t:
                return p
        return p

    def _common_dept(self, a, b):
        common = [d for d in self.pop.depts[a].items if d in self.pop.dept_set[b]]
        return self.rng.choice(common) if common else "Médecine générale"

    def _scn_a(self, t):
        rng, pop = self.rng, self.pop
        patient = pop.grp_a[self.a_count % len(pop.grp_a)]
        self.a_count += 1
        group = pop.fac[pop.by_city[pop.home[patient]][0]]["hospital_group"]
        cands = [i for i in pop.by_group[group] if i != pop.grp_b and i not in pop.grp_c]
        n = rng.choice([4, 5])
        chain = rng.sample(cands, n + 1)
        self._visit(t, patient, chain[0], "Médecine générale", 3, {"plan": chain[1:], "short": True})

    # ---- destination d'un transfert -------------------------------------
    def _destination(self, f):
        rng, pop = self.rng, self.pop
        fac = pop.fac[f]
        g, ftype = fac["hospital_group"], fac["type"]
        if ftype == "CHU":
            others = [c for gg in pop.groups for c in pop.chu[gg] if c != f]
            same = [c for c in others if pop.fac[c]["hospital_group"] == g]
            return rng.choice(same if same and rng.random() < 0.6 else others)
        if rng.random() < P_CROSS_CHU:
            others = [c for gg in pop.groups if gg != g for c in pop.chu[gg]]
            if others:
                return rng.choice(others)
        if g == pop.fac[pop.grp_b]["hospital_group"] and f != pop.grp_b and rng.random() < P_SCN_B:
            return pop.grp_b
        if ftype == "CENTRE_SANTE":
            want = "HOPITAL_REGIONAL" if rng.random() < 0.55 else "CHU"
        elif ftype == "CLINIQUE":
            want = "CHU" if rng.random() < 0.7 else "HOPITAL_REGIONAL"
        else:
            want = "CHU" if rng.random() < 0.85 else "HOPITAL_REGIONAL"
        cands = [i for i in pop.by_group[g] if i != f and pop.fac[i]["type"] == want]
        if not cands:
            cands = [i for i in pop.chu[g] if i != f] or [i for gg in pop.groups for i in pop.chu[gg]]
        local = [i for i in cands if pop.fac[i]["city"] == fac["city"]]
        if local and rng.random() < 0.9:
            cands = local
        return Picker(cands, [pop.fac[i]["beds"] for i in cands]).pick(rng)

    # ---- déroulement d'un passage ---------------------------------------
    def _visit(self, t0, patient, f, dept, prio, ctx):
        rng, pop = self.rng, self.pop
        t0 = float(int(t0))                      # horodatages à la seconde
        fac = pop.fac[f]
        ftype = fac["type"]
        vid = f"V-{self.visit_no:07d}"
        self.visit_no += 1
        self.stats["visits"] += 1
        base = {"visit_id": vid, "patient_id": patient, "facility_id": f, "city": fac["city"]}

        def ev(status, ts, priority=None, wait=None, desk=None, transfer_to=None):
            e = dict(base)
            e.update({"status": status, "priority": priority, "wait_minutes": wait, "department": dept,
                      "desk_id": desk, "transfer_to": transfer_to, "event_time": iso(ts)})
            self._emit(ts, e)

        ev("ARRIVED", t0)
        if prio is None:
            prio = rng.choices([1, 2, 3, 4, 5], PRIO_MIX.get(ftype, PRIO_MIX["CLINIQUE"]))[0]
        load = self._load(t0)
        t1 = t0 + (rng.uniform(30, 180) if prio == 1 else rng.uniform(120, 1200) * load)
        ev("TRIAGED", t1, priority=prio, desk=pop.desks[f].pick(rng))
        if prio >= 4 and not ctx and rng.random() < P_LWBS:
            self.stats["lwbs"] += 1
            self.busy[patient] = t1
            return                                   # parti sans être pris en charge
        med, sigma = WAIT[prio]
        w = med * rng.lognormvariate(0.0, sigma) * WAIT_FACTOR.get(ftype, 1.0) * load
        t2 = t0 + min(MAX_WAIT_MIN, max(1.0, w)) * 60 + rng.uniform(0, 59)
        if t2 < t1 + 60:
            t2 = t1 + rng.uniform(60, 300)
        t2 = float(int(t2))
        wait = int((t2 - t0) // 60)
        if wait >= 60 and rng.random() < P_WAIT_TYPO:
            wait = (wait // 60) * 100 + wait % 60
            self.stats["wait_typo"] += 1
        ev("CARE_STARTED", t2, priority=prio, wait=wait)

        # issue du passage
        dest, next_ctx = None, {}
        plan = ctx.get("plan")
        if ctx.get("error") == "self":
            dest, next_ctx = f, None
            self.stats["self_transfer"] += 1
        elif ctx.get("error") == "unknown":
            dest, next_ctx = f"F-{rng.randint(41, 49):03d}", None
            self.stats["unknown_facility"] += 1
        elif plan:
            dest = plan[0]
            next_ctx = {"plan": plan[1:], "short": ctx.get("short", False)} if len(plan) > 1 else {"discharge": True}
        elif ctx.get("return_to"):
            dest, next_ctx = ctx["return_to"], {"discharge": True}
        elif not ctx.get("discharge"):
            origin = ctx.get("origin")
            if origin and ftype == "CHU" and rng.random() < P_RETURN:
                dest, next_ctx = origin, {"discharge": True}
            else:
                p = P_TRANSFER.get(ftype, 0.05) * PRIO_TRANSFER[prio]
                if dept in SPECIALTY and ftype in ("CLINIQUE", "CENTRE_SANTE"):
                    p *= 1.6
                if origin:
                    p *= 0.5
                if rng.random() < p:
                    dest = self._destination(f)
                    next_ctx = {"origin": f}

        if ctx.get("short"):
            t3 = t2 + rng.uniform(1800, 7200)
        elif prio <= 2:
            t3 = t2 + rng.uniform(3600, 6 * 3600)
        else:
            t3 = t2 + rng.uniform(1200, 3 * 3600)
        self.busy[patient] = max(self.busy.get(patient, 0), t3)
        if dest is None:
            ev("DISCHARGED", t3, priority=prio)
            return
        ev("TRANSFERRED", t3, priority=prio, transfer_to=dest)
        self.stats["transfers"] += 1
        if next_ctx is None or dest not in pop.fac:
            return                                   # erreur de saisie : pas de passage à l'arrivée
        same_city = pop.fac[dest]["city"] == fac["city"]
        t_arr = t3 + (rng.uniform(900, 3600) if same_city else rng.uniform(2 * 3600, 5 * 3600))
        if "plan" not in next_ctx and "discharge" not in next_ctx:
            next_ctx["origin"] = f
        self.busy[patient] = t_arr
        new_prio = prio if rng.random() < 0.7 else max(1, min(5, prio + rng.choice([-1, 1])))
        self._push(t_arr, "VISIT", (patient, dest, dept, new_prio, next_ctx))

    def _emit(self, ts, event):
        rng = self.rng
        arrival = ts
        if rng.random() < P_LATE:
            arrival = ts + rng.uniform(30, MAX_LATE_S)
            self.stats["late"] += 1
        self._push(arrival, "EMIT", event)
        if rng.random() < P_DUP:
            self._push(arrival + rng.uniform(1, 90), "EMIT", event)
            self.stats["dup"] += 1

    # ---- boucle principale ----------------------------------------------
    def events(self):
        """Génère indéfiniment des couples (instant_d_arrivée, événement), dans l'ordre d'arrivée."""
        rng = self.rng
        while True:
            ts, _, kind, payload = heapq.heappop(self.heap)
            self.t = ts
            if kind == "EMIT":
                yield ts, payload
            elif kind == "NEW":
                accept = True
                if self.use_profile:
                    h = datetime.fromtimestamp(ts, tz=timezone.utc).hour
                    accept = rng.random() < self.visits_per_s * HOURLY[h] / MEAN_HOURLY / self.lam_max
                if accept:
                    self._new_visit(ts)
                self._push(ts + rng.expovariate(self.lam_max), "NEW", None)
            elif kind == "VISIT":
                patient, f, dept, prio, ctx = payload
                self._visit(ts, patient, f, dept, prio, ctx)


# Nombre moyen d'événements émis par nouveau passage, transferts et doublons compris (sert à caler le débit)
EVENTS_PER_VISIT = 4.10


def load_facilities(path):
    with open(path, encoding="utf-8") as f:
        txt = f.read().strip()
    if txt.startswith("["):
        return json.loads(txt)
    return [json.loads(line) for line in txt.splitlines() if line.strip()]


# --------------------------------------------------------------------------
# Modes
# --------------------------------------------------------------------------
def run_file(args, pop, rng):
    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    visits_per_s = args.n / EVENTS_PER_VISIT / (args.days * 86400)
    sim = Simulator(pop, rng, start.timestamp(), visits_per_s, use_profile=True)
    status = Counter()
    visits = set()
    first = last = None
    with open(args.output, "w", encoding="utf-8") as f:
        for i, (arrival, ev) in enumerate(sim.events()):
            if i >= args.n:
                break
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")
            status[ev["status"]] += 1
            visits.add(ev["visit_id"])
            first = first or ev["event_time"]
            last = ev["event_time"]
    print(f"{args.n} événements écrits dans {args.output}", file=sys.stderr)
    print(f"  période couverte : {first} -> {last}", file=sys.stderr)
    print(f"  statuts : {dict(status)}", file=sys.stderr)
    print(f"  passages : {len(visits)}  (en retard : {sim.stats['late']}, doublons : {sim.stats['dup']})",
          file=sys.stderr)


class KafkaSink:
    def __init__(self, bootstrap, topic):
        self.topic = topic
        try:
            from kafka import KafkaProducer  # kafka-python
            self.kind = "kafka-python"
            try:
                self.p = KafkaProducer(bootstrap_servers=bootstrap.split(","), acks="all", linger_ms=20)
            except Exception as exc:  # NoBrokersAvailable, etc.
                sys.exit(f"Erreur : impossible de joindre Kafka sur {bootstrap} ({exc}). "
                         "Vérifiez --bootstrap (localhost:9092 depuis la machine hôte, "
                         "kafka:9092 depuis un conteneur du même réseau Docker).")
        except ImportError:
            try:
                from confluent_kafka import Producer  # confluent-kafka
            except ImportError:
                sys.exit("Erreur : installez kafka-python (pip install kafka-python) "
                         "ou confluent-kafka, ou utilisez --dry-run.")
            self.kind = "confluent-kafka"
            self.p = Producer({"bootstrap.servers": bootstrap, "acks": "all", "linger.ms": 20})

    def send(self, key, value):
        if self.kind == "kafka-python":
            self.p.send(self.topic, key=key.encode("utf-8"), value=value.encode("utf-8"))
        else:
            while True:
                try:
                    self.p.produce(self.topic, key=key.encode("utf-8"), value=value.encode("utf-8"))
                    break
                except BufferError:
                    self.p.poll(0.5)
            self.p.poll(0)

    def close(self):
        self.p.flush()
        if self.kind == "kafka-python":
            self.p.close()


class StdoutSink:
    def send(self, key, value):
        sys.stdout.write(value + "\n")
        sys.stdout.flush()

    def close(self):
        pass


def run_kafka(args, pop, rng):
    sink = StdoutSink() if args.dry_run else KafkaSink(args.bootstrap, args.topic)
    t0_wall = time.time()
    t0_sim = t0_wall
    visits_per_s = args.rate / EVENTS_PER_VISIT / args.speed
    # visit_id dérivés de l'heure de lancement : deux lancements successifs ne réutilisent pas les mêmes
    counter = int(t0_wall - 1_790_000_000) * 100
    # Préchauffage : la simulation démarre quelques heures (simulées) plus tôt et les événements antérieurs
    # au lancement ne sont pas envoyés. Le flux démarre ainsi « en régime établi », avec des passages en
    # cours. Durée bornée pour que le démarrage reste rapide même avec un --rate élevé.
    warmup = min(6 * 3600.0, 600000 * args.speed / args.rate)
    sim = Simulator(pop, rng, t0_sim - warmup, visits_per_s, use_profile=False, visit_counter=counter)
    target = "stdout (dry-run)" if args.dry_run else f"{args.bootstrap} / topic {args.topic}"
    print(f"Envoi vers {target} : ~{args.rate} événements/s (Ctrl+C pour arrêter)", file=sys.stderr)
    print("Préchauffage de la simulation (quelques secondes) ...", file=sys.stderr)

    stop = {"flag": False}
    signal.signal(signal.SIGINT, lambda *_: stop.__setitem__("flag", True))
    signal.signal(signal.SIGTERM, lambda *_: stop.__setitem__("flag", True))

    sent = 0
    status = Counter()
    last_log, last_sent = time.time(), 0
    for arrival, ev in sim.events():
        if stop["flag"]:
            break
        if arrival < t0_sim:  # préchauffage
            continue
        wall_due = t0_wall + (arrival - t0_sim) / args.speed
        delay = wall_due - time.time()
        while delay > 0 and not stop["flag"]:
            time.sleep(min(delay, 0.5))
            delay = wall_due - time.time()
        if stop["flag"]:
            break
        sink.send(ev["visit_id"], json.dumps(ev, ensure_ascii=False))
        sent += 1
        status[ev["status"]] += 1
        now = time.time()
        if now - last_log >= 10:
            print(f"[{iso(now)}] {sent} événements envoyés ({(sent - last_sent) / (now - last_log):.1f}/s)",
                  file=sys.stderr)
            last_log, last_sent = now, sent
        if args.max_events and sent >= args.max_events:
            break
        if args.duration and now - t0_wall >= args.duration:
            break
    sink.close()
    elapsed = max(time.time() - t0_wall, 1e-9)
    print(f"Terminé : {sent} événements en {elapsed:.0f} s ({sent / elapsed:.1f}/s), statuts : {dict(status)}",
          file=sys.stderr)


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description="Générateur de passages aux urgences UrgenceNet.ma")
    ap.add_argument("--mode", choices=["file", "kafka"], required=True)
    ap.add_argument("--facilities", default=os.path.join(here, "facilities.json"),
                    help="référentiel des établissements (défaut : facilities.json à côté du script)")
    ap.add_argument("--seed", type=int, default=42, help="graine aléatoire (défaut : 42)")
    g = ap.add_argument_group("mode file")
    g.add_argument("--n", type=int, default=50000, help="nombre d'événements (défaut : 50000)")
    g.add_argument("--output", default="urgences_history.jsonl", help="fichier de sortie")
    g.add_argument("--start", default="2026-11-09", help="premier jour simulé, AAAA-MM-JJ (défaut : 2026-11-09)")
    g.add_argument("--days", type=float, default=7, help="nombre de jours simulés (défaut : 7)")
    g = ap.add_argument_group("mode kafka")
    g.add_argument("--rate", type=float, default=20, help="événements par seconde (défaut : 20)")
    g.add_argument("--bootstrap", default="localhost:9092",
                   help="serveur(s) Kafka (défaut : localhost:9092 ; kafka:9092 depuis un conteneur)")
    g.add_argument("--topic", default="urgences", help="topic Kafka (défaut : urgences)")
    g.add_argument("--speed", type=float, default=1.0,
                   help="accélération du temps simulé (défaut : 1 = temps réel)")
    g.add_argument("--max-events", type=int, default=0, help="arrêt après N événements (0 = sans fin)")
    g.add_argument("--duration", type=float, default=0, help="arrêt après N secondes (0 = sans fin)")
    g.add_argument("--dry-run", action="store_true", help="affiche les événements au lieu de les envoyer")
    args = ap.parse_args()

    pop = Population(load_facilities(args.facilities), args.seed)
    rng = random.Random(args.seed + 1)
    if args.mode == "file":
        run_file(args, pop, rng)
    else:
        run_kafka(args, pop, rng)


if __name__ == "__main__":
    main()
