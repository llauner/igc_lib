import main_tracemap



def launch_tracemap(request):
    main_tracemap.main(request)

def launch_heatmap(request):
    main_heatmap.main(request)



if __name__ == "__main__":
    try:
        res = launch_tracemap(None)
        print(res)
    except SystemExit as e:
        if not e is None:
            print(e)