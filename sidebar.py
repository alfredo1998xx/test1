# sidebar.py
import streamlit as st

def render_sidebar():
    # Inject Lucide icon CDN
    st.markdown("""
        <script src="https://unpkg.com/lucide@latest"></script>
        <style>
        [data-testid="stSidebar"] {
            background-color: #2e2e2e;
            padding-top: 1rem;
        }

        .nav-item {
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 10px 16px;
            margin-bottom: 8px;
            border-radius: 8px;
            font-size: 16px;
            color: white;
            cursor: pointer;
            transition: background-color 0.2s ease;
        }

        .nav-item:hover {
            background-color: #444;
        }

        .nav-item.selected {
            background-color: #C53131;
            font-weight: bold;
        }

        .lucide-icon {
            width: 20px;
            height: 20px;
            stroke: white;
        }
        </style>
    """, unsafe_allow_html=True)

    menu_items = {
        "Dashboard": "bar-chart-2",
        "Employees": "user",
        "Structure": "folder",
        "Actual Hours": "clock",
        "Room STATs": "building",
        "Scheduling": "calendar-days",
        "Cost and OT Mgmt": "dollar-sign",
        "Reports": "file-text"
    }

    st.sidebar.markdown('<h5 style="color:white;">Menu</h5>', unsafe_allow_html=True)

    for label, icon in menu_items.items():
        is_selected = st.session_state.get("main_page", "Dashboard") == label
        div_class = "nav-item selected" if is_selected else "nav-item"

        clicked = st.sidebar.button(" ", key=f"btn_{label}")
        st.sidebar.markdown(f"""
            <div class="{div_class}" onclick="window.location.reload();document.querySelector('input[data-testid=baseButton][aria-label=\'{label}\']').click();">
                <i data-lucide="{icon}" class="lucide-icon"></i>
                <span>{label}</span>
            </div>
            <script>
                lucide.createIcons();
            </script>
        """, unsafe_allow_html=True)

        if clicked:
            st.session_state.main_page = label