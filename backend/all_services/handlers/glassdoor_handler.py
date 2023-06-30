from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from ..glassdoor_service.glassdoor_scraping import main
import pandas as pd

@csrf_exempt
def scrape_glassdoor_view(request):
    if request.method == 'POST':
        url = request.POST.get('url')
        csv_file = request.FILES.get('csv_file')

        if url and csv_file:
            return JsonResponse({"error": "Please provide either a single URL or a CSV file, not both."}, status=400)
        elif url:
            main(url=url)
        elif csv_file:
            with open('company_urls.csv', 'wb') as file:
                for chunk in csv_file.chunks():
                    file.write(chunk)
            main(csv_file='company_urls.csv')
        else:
            return JsonResponse({"error": "Please provide either a single URL or a CSV file."}, status=400)

        return JsonResponse({"message": "Scraping and data storage successful."})
    else:
        return JsonResponse({"error": "POST request required."}, status=400)