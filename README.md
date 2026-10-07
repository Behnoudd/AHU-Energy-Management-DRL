# Industrial Peak Shaving using Deep Reinforcement Learning (DQN)

![PLC SCADA Architecture](docs/plc_scada_architecture.jpg)

## 📌 Project Overview
An end-to-end industrial energy management system built according to the **CRISP-DM** methodology. This project leverages **Deep Reinforcement Learning (DQN)** in PyTorch and Gymnasium to perform automatic peak shaving on industrial HVAC equipment (`AHU_A`), eliminating expensive peak demand penalties without disrupting production operations.

---

## 🚀 Key Performance Indicators (KPIs)
* **100% Demand Violation Reduction:** Reduced peak load violations from **795 occurrences down to 0** on evaluation dataset.
* **Financial Savings:** Saved an estimated **~39.75 Million Tomans** in utility penalty fees.
* **Proactive Load Shaving:** Agent applies dynamic 10% and 20% load reduction actions only during critical demand surges.

---

## 📊 Evaluation & Performance
![DRL Performance](docs/drl_evaluation_results.png)

---

## 🛠️ Tech Stack & Methodology
* **Framework:** CRISP-DM Methodology
* **RL Framework:** Deep Q-Network (DQN), Replay Buffer, Target Network
* **Libraries:** Python, PyTorch, Gymnasium, Pandas, NumPy, Matplotlib
* **Industrial Protocols:** Modbus TCP, PLC/SCADA Integration Architecture

---

## 💻 How to Run
1. Clone the repository:
   ```bash
   git clone [https://github.com/YOUR_USERNAME/AHU-Energy-Management-DRL.git](https://github.com/YOUR_USERNAME/AHU-Energy-Management-DRL.git)
   cd AHU-Energy-Management-DRL




