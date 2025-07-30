# SmartSolver — StorCLI Full Log Sorting Tool

SmartSolver parses StorCLI full logs and extracts useful information such as SMART parameters, device temperatures, and access point details, saving everything into a structured Excel file. (HPDS purpose)

---

## 🛠 How to Use

1. **Place the files properly:**
   - **Windows:** Place `extractor.exe` in the same directory as the script.
   - **Linux:** Place the `extractor` binary alongside the script.

2. **Run the script.**

3. **Results:**
   - Output is generated as an Excel file.
   - Includes:
     - Device temperature
     - SMART parameters
     - Access point details

---

## 📦 Required Python Libraries

Make sure the following libraries are installed:

```bash
pip install pandas openpyxl
