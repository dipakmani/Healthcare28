import os
import csv
import random
from datetime import date, timedelta
from faker import Faker
import pandas as pd

# ---------------------------
# Config
# ---------------------------
SEED = 42
TOTAL_ROWS = 500_000
CHUNK_SIZE = 50_000
FULL_CSV = "fact_visits_500k.csv"

# Patient/Visit rules
NUM_PATIENTS = 3000  # unique patients
MIN_VISITS = 2
MAX_VISITS = 4

# Hospitals/Departments/Doctors/Insurers
NUM_HOSPITALS = 50
NUM_DEPARTMENTS = 10
NUM_DIAGNOSES = 10
NUM_DOCTORS = 100
NUM_INSURERS = 20

# Countries and States mapping
COUNTRY_STATES = {
    "India": ["Maharashtra", "Karnataka", "Delhi", "Gujarat", "Tamil Nadu"],
    "USA": ["California", "Texas", "New York", "Florida", "Illinois"],
    "UK": ["England", "Scotland", "Wales", "Northern Ireland", "London"],
    "Australia": ["New South Wales", "Victoria", "Queensland", "Western Australia", "Tasmania"],
    "Canada": ["Ontario", "Quebec", "British Columbia", "Alberta", "Manitoba"]
}

# Hospital provider names by country
HOSPITAL_PROVIDERS = {
    "India": ["Apollo Hospitals", "Fortis Healthcare", "Max Healthcare", "Manipal Hospitals", "Narayana Health"],
    "USA": ["Mayo Clinic", "Cleveland Clinic", "Johns Hopkins Medicine", "Mass General Brigham", "UCLA Health"],
    "UK": ["NHS Trust", "Bupa UK", "Spire Healthcare", "Nuffield Health", "Circle Health Group"],
    "Australia": ["Ramsay Health Care", "St Vincent’s Health", "Healthscope", "Mater Health Services", "Calvary Health Care"],
    "Canada": ["Maple Health Network", "Medisys Health Group", "North York General", "Sunnybrook Health", "Unity Health Toronto"]
}

# ---------------------------
# Faker setup
# ---------------------------
random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

def calc_age(dob: date, on_date: date) -> int:
    years = on_date.year - dob.year - ((on_date.month, on_date.day) < (dob.month, dob.day))
    return max(0, years)

# ---------------------------
# Generate fixed pools
# ---------------------------
# Hospitals
hospital_pool = []
for i in range(1, NUM_HOSPITALS + 1):
    country = random.choice(list(COUNTRY_STATES.keys()))
    state = random.choice(COUNTRY_STATES[country])
    hospital_pool.append({
        "hospitalid": f"H{i:03d}",
        "hospitalname": f"{fake.last_name()} {random.choice(['Medical Center', 'General Hospital', 'Clinic', 'Specialty Hospital'])}",
        "hospitalprovidername": random.choice(HOSPITAL_PROVIDERS[country]),
        "hospitaltype": random.choice(["General", "Specialty"]),
        "accreditation_level": random.choice(["Level A", "Level B", "Level C"]),
        "bed_capacity": random.randint(100, 1000),
        "hospital_address": fake.street_address(),
        "hospitalstate": state,
        "hospitalcity": fake.city(),
        "hospitalcountry": country,
        "hospitalpostalcode": fake.postcode()
    })

# Departments
dept_names = ["Cardiology","Orthopedics","Neurology","Dermatology","Pediatrics",
              "ENT","Gastroenterology","Respiratory","Oncology","General Medicine"]
dept_specialities = ["Heart","Bones","Brain","Skin","Children",
                     "Ear Nose Throat","Digestive","Pulmonary","Cancer","Primary Care"]
department_pool = [
    {"departmentid": f"DEP{i+1:02d}", "departmentname": dept_names[i], "deptspeciality": dept_specialities[i]}
    for i in range(NUM_DEPARTMENTS)
]

# Diagnoses
diagnosis_pool = []
for i in range(1, NUM_DIAGNOSES + 1):
    diagnosis_pool.append({
        "diagnosisid": f"DIAG{i:02d}",
        "diagnosiscode": f"ICD{i:03d}",
        "diagnosisdiscription": f"Diagnosis {i} description",
        "diagnosiscategory": dept_specialities[i-1]
    })

# Doctors
doctor_pool = []
for i in range(1, NUM_DOCTORS + 1):
    hosp = random.choice(hospital_pool)
    dept = random.choice(department_pool)
    full_name = fake.name()
    doctor_pool.append({
        "Doctorid": f"D{i:03d}",
        "doctorfullname": full_name,
        "doctor_specialization": dept["departmentname"],
        "doctorcontactnumber": fake.phone_number(),
        "doctoryears_experience": random.randint(3, 35),
        "doctoremail": f"{full_name.lower().replace(' ','')}@hospital.com",
        "doctor_shift_type": random.choice(["Morning","Evening","Night"]),
        "assigned_hospitalid": hosp["hospitalid"],
        "assigned_departmentid": dept["departmentid"]
    })

# Insurance
insurance_pool = []
for i in range(1, NUM_INSURERS + 1):
    insurance_pool.append({
        "insuranceproviderid": f"INS{i:03d}",
        "insuranceprovidername": f"Insurance_{i}",
        "insuranceplan_type": random.choice(["Gold","Silver","Platinum"]),
        "coverage_percentage": random.randint(50, 90),
        "contact_number": fake.phone_number(),
        "insurance_email": f"claims{i}@insureco.com"
    })

# ---------------------------
# Generate patients (unique)
# ---------------------------
blood_groups = ["A+","A-","B+","B-","O+","O-","AB+","AB-"]
patients = []
for i in range(1, NUM_PATIENTS+1):
    country = random.choice(list(COUNTRY_STATES.keys()))
    state = random.choice(COUNTRY_STATES[country])
    dob = fake.date_of_birth(minimum_age=1, maximum_age=95)
    patients.append({
        "patientid": f"P{i:05d}",
        "patient_fullname": fake.name(),
        "patientgender": random.choice(["M","F"]),
        "patientdob": dob,
        "patientaddress": fake.street_address(),
        "patientcity": fake.city(),
        "patientstate": state,
        "patientcountry": country,
        "patientpostalcode": fake.postcode(),
        "patient_satisfactionsxore": round(random.uniform(1,5),1),
        "patient_bloodgroup": random.choice(blood_groups)
    })

# ---------------------------
# Columns
# ---------------------------
COLUMNS = [
    "visitid","admission_date","discharge_date","patientid","patient_fullname","patientgender","age","patientdob",
    "patientaddress","patientcity","patientstate","patientcountry","patientpostalcode","patient_satisfactionsxore",
    "patient_bloodgroup","patientwaittime","Doctorid","doctorfullname","doctor_specialization","doctorcontactnumber",
    "doctoryears_experience","doctoremail","doctor_shift_type","departmentid","departmentname","deptspeciality",
    "Departmentreferral","floor_number","hospitalid","hospitalname","hospitalprovidername","hospitaltype","accreditation_level",
    "bed_capacity","hospital_address","hospitalstate","hospitalcity","hospitalcountry","hospitalpostalcode",
    "total_billing_amount","insurancecoveredamount","patient_covered_amount","full_date","diagnosisid","diagnosiscode",
    "diagnosisdiscription","diagnosiscategory","insuranceproviderid","insuranceprovidername","insuranceplan_type",
    "coverage_percentage","contact_number","insurance_email"
]

# ---------------------------
# Make visit row
# ---------------------------
def make_visit_row(visit_num, patient):
    admission_date = fake.date_between(start_date="-3y", end_date="today")
    discharge_date = admission_date + timedelta(days=random.randint(0,15))
    full_date = admission_date
    age = calc_age(patient["patientdob"], full_date)
    
    hosp = random.choice(hospital_pool)
    dept = random.choice(department_pool)
    diag = random.choice(diagnosis_pool)
    doc = random.choice(doctor_pool)
    insurer = random.choice(insurance_pool)
    
    total_bill = random.randint(500,20000)
    insurance_cover = int(total_bill * insurer["coverage_percentage"]/100)
    patient_cover = total_bill - insurance_cover
    
    return {
        "visitid": f"V{visit_num:07d}",
        "admission_date": admission_date.isoformat(),
        "discharge_date": discharge_date.isoformat(),
        "patientid": patient["patientid"],
        "patient_fullname": patient["patient_fullname"],
        "patientgender": patient["patientgender"],
        "age": age,
        "patientdob": patient["patientdob"].isoformat(),
        "patientaddress": patient["patientaddress"],
        "patientcity": patient["patientcity"],
        "patientstate": patient["patientstate"],
        "patientcountry": patient["patientcountry"],
        "patientpostalcode": patient["patientpostalcode"],
        "patient_satisfactionsxore": patient["patient_satisfactionsxore"],
        "patient_bloodgroup": patient["patient_bloodgroup"],
        "patientwaittime": random.randint(5,120),
        "Doctorid": doc["Doctorid"],
        "doctorfullname": doc["doctorfullname"],
        "doctor_specialization": doc["doctor_specialization"],
        "doctorcontactnumber": doc["doctorcontactnumber"],
        "doctoryears_experience": doc["doctoryears_experience"],
        "doctoremail": doc["doctoremail"],
        "doctor_shift_type": doc["doctor_shift_type"],
        "departmentid": dept["departmentid"],
        "departmentname": dept["departmentname"],
        "deptspeciality": dept["deptspeciality"],
        "Departmentreferral": random.choice(["Self","Referral","Follow-up"]),
        "floor_number": random.randint(1,7),
        "hospitalid": hosp["hospitalid"],
        "hospitalname": hosp["hospitalname"],
        "hospitalprovidername": hosp["hospitalprovidername"],
        "hospitaltype": hosp["hospitaltype"],
        "accreditation_level": hosp["accreditation_level"],
        "bed_capacity": hosp["bed_capacity"],
        "hospital_address": hosp["hospital_address"],
        "hospitalstate": hosp["hospitalstate"],
        "hospitalcity": hosp["hospitalcity"],
        "hospitalcountry": hosp["hospitalcountry"],
        "hospitalpostalcode": hosp["hospitalpostalcode"],
        "total_billing_amount": total_bill,
        "insurancecoveredamount": insurance_cover,
        "patient_covered_amount": patient_cover,
        "full_date": full_date.isoformat(),
        "diagnosisid": diag["diagnosisid"],
        "diagnosiscode": diag["diagnosiscode"],
        "diagnosisdiscription": diag["diagnosisdiscription"],
        "diagnosiscategory": diag["diagnosiscategory"],
        "insuranceproviderid": insurer["insuranceproviderid"],
        "insuranceprovidername": insurer["insuranceprovidername"],
        "insuranceplan_type": insurer["insuranceplan_type"],
        "coverage_percentage": insurer["coverage_percentage"],
        "contact_number": insurer["contact_number"],
        "insurance_email": insurer["insurance_email"]
    }

# ---------------------------
# Write CSV
# ---------------------------
def write_csv_header(path):
    with open(path,"w",newline="",encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        
def append_csv_rows(path, rows):
    with open(path,"a",newline="",encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writerows(rows)

# ---------------------------
# Generate full 500k dataset
# ---------------------------
def generate_full():
    write_csv_header(FULL_CSV)
    visit_counter = 1
    buffer = []
    
    # Each patient will have 2-4 visits
    patient_visits = []
    for patient in patients:
        visits_count = random.randint(MIN_VISITS, MAX_VISITS)
        for _ in range(visits_count):
            patient_visits.append(patient)
    
    # Shuffle to randomize order
    random.shuffle(patient_visits)
    
    # Trim/expand to TOTAL_ROWS
    while len(patient_visits) < TOTAL_ROWS:
        patient_visits.extend(random.choices(patients, k=TOTAL_ROWS - len(patient_visits)))
    patient_visits = patient_visits[:TOTAL_ROWS]
    
    for patient in patient_visits:
        buffer.append(make_visit_row(visit_counter, patient))
        visit_counter += 1
        
        if len(buffer) >= CHUNK_SIZE:
            append_csv_rows(FULL_CSV, buffer)
            buffer.clear()
            print(f"... wrote up to visit {visit_counter-1:,}")
    
    if buffer:
        append_csv_rows(FULL_CSV, buffer)
    
    print(f"✅ Finished writing {TOTAL_ROWS:,} visits to {FULL_CSV}")

# ---------------------------
# Main
# ---------------------------
if __name__ == "__main__":
    generate_full()
