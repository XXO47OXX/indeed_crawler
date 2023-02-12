import re

# HELPER FUNCTIONS
# Format the salary column by creating a function that splits the salary string intro two columns
def salary_type_func(salary):
    if salary is None:
        return None
    else:
        if salary.find("year") != -1:
            return "year"
        elif salary.find("hour") != -1:
            return "hour"
        elif salary.find("month") != -1:
            return "month"
        elif salary.find("week") != -1:
            return "week"
        else:
            return None

# Define a function that return the higher end of the salary
def salary_high_func(salary, salary_type):
    if salary is None:
        return None
    else:
        if salary.find("–") != -1: # Type 1: $55,000–$62,000 a year
            return float(re.findall(pattern=f"(?<=–\$).*(?=\sa\s{salary_type}|\san\s{salary_type})", string=salary)[0].replace(",", ""))
        elif salary.find("From") != -1: # Type 2: From $80,000 a year
            return None
        elif salary.find("Up") != -1: # Type 3: Up to $160,000 a year
            return float(re.findall(pattern=f"(?<=Up\sto\s\$).*(?=\sa\s{salary_type}|\san\s{salary_type})", string=salary)[0].replace(",", ""))
        elif any(x in salary for x in ["-", "From", "Up"]) == False: # Type 4: $150,000 a year
            return float(re.findall(pattern=f"(?<=\$).*(?=\sa\s{salary_type}|\san\s{salary_type})", string=salary)[0].replace(",", ""))
        else:
            return None

# Define a function that returns the lower end of the salary
def salary_low_func(salary, salary_type):
    if salary is None:
        return None
    else:
        if salary.find("–") != -1: # Type 1: $55,000–$62,000 a year
            return float(re.findall(pattern="(?<=\$).*(?=\–\$)", string=salary)[0].replace(",", ""))
        elif salary.find("From") != -1: # Type 2: From $80,000 a year
            return float(re.findall(pattern=f"(?<=From\s\$).*(?=\sa\s{salary_type}|\san\s{salary_type})", string=salary)[0].replace(",", ""))
        elif salary.find("Up") != -1: # Type 3: Up to $160,000 a year
            return None
        elif any(x in salary for x in ["-", "From", "Up"]) is False: # Type 4: $150,000 a year
            return float(re.findall(pattern=f"(?<=\$).*(?=\sa\s{salary_type}|\san\s{salary_type})", string=salary)[0].replace(",", ""))
        else:
            return None