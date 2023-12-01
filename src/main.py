import libs.features as ft

if __name__ == '__main__':

    data = ft.scan_reports(
        directory='reports'
    )

    for index, row in data.iterrows():
        if ft.check_report(row=row):
            issue_key = ft.create_ticket(row=row, project_key='RA')

            # Agora, atualize a coluna 'JiraTicketKey' no DataFrame
            data.at[index, 'JiraTicketKey'] = issue_key

            ft.save_report(
                table_name='reports',
                row=row
            )

    ft.scan_ticket()

    # ft.update_report()
