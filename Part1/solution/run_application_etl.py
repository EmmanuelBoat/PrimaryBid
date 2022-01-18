from PrimaryBid.Part1.solution.application_etl import ApplicationLifecycle


def execute_transformation():
    raw_file = "/Users/emmanuelsifah/Downloads/primarybidData/CC Application Lifecycle.csv"
    app_lifecycle = ApplicationLifecycle(app_filepath=raw_file)
    app_lifecycle.transform_app_data()


if __name__ == '__main__':
    execute_transformation()
