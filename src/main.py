import config.settings as st
import libs.features as ft

if __name__ == '__main__':

    df = ft.scan_reports(directory='reports')

    ft.save_new_report(df)

    df = ft.create_ticket(
        query_table='reports',
        project_key=st.jira_project,
    )

    ft.update_report(df)

    tickets = ft.scan_ticket()
    ft.update_ticket(tickets)
