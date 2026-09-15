# Auto_updater
Automated Streamlit platform for Kimai time tracking. It checks PostgreSQL article counts, skips 2026 holidays and weekends, deterministically rotates projects, activities, and split shifts, and auto-submits two 4-hour daily timesheet slots via the Kimai REST API either on-demand or through an 18:30 background daemon.
