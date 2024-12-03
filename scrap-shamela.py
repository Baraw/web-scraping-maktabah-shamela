import requests
from bs4 import BeautifulSoup
import pandas as pd
import csv
import concurrent.futures
import os
import re
from tqdm import tqdm

# Fonction pour vérifier si un fichier existe déjà et générer un nom unique
def get_available_filename(base_path, extension):
    suffix = 1
    file_path = f"{base_path}{extension}"
    while os.path.exists(file_path):
        file_path = f"{base_path}_{suffix}{extension}"
        suffix += 1
    return file_path

# Fonction pour récupérer le contenu d'une page
def request_page(id_buku, nomor_halaman):
    URL = f"https://shamela.ws/book/{id_buku}/{nomor_halaman}"
    try:
        page = requests.get(URL)
    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de la récupération de la page {nomor_halaman}: {e}")
        return None

    if page.status_code != 200:
        print(f"Erreur de récupération de la page {nomor_halaman}: Code {page.status_code}")
        return None  # Retourner None pour signaler l'erreur

    soup = BeautifulSoup(page.content, "html.parser")
    
    # Extraire la sourate depuis le div "size-12"
    heading_div = soup.find("div", class_="size-12")
    sourate = "غير معروف"
    if heading_div:
        sourate_links = heading_div.find_all("a")
        if len(sourate_links) >= 2:
            sourate_text = sourate_links[1].text.strip()
            sourate = sourate_text.replace("(", "").replace(")", "")
    
    # Trouver le bloc 'wrapper'
    results = soup.find(id="wrapper")
    if results is None:
        print(f"L'élément avec l'ID 'wrapper' n'a pas été trouvé sur la page {nomor_halaman}.")
        return None
    
    job_elements = results.find_all("div", class_="nass")
    
    data = []  # Liste pour stocker les informations extraites
    previous_verset_number = ""  # Pour garder la trace du numéro de verset précédent
    previous_entry = None  # Pour garder la référence à l'entrée précédente

    for job_element in job_elements:
        # Extraire les versets
        verset_elements = job_element.find_all("p")
        
        for p in verset_elements:
            # Initialiser les variables
            verset_number = ""
            termes = ""
            narrateurs_principaux = ""
            reste_narrateurs = ""
            texte_entier = p.get_text(separator=" ", strip=True)
            
            # Vérifier si le paragraphe contient "قوله تعالى:" dans 'c5'
            c5_span = p.find("span", class_="c5")
            has_qawl = False
            if c5_span and "قوله تعالى:" in c5_span.text:
                has_qawl = True
            elif "قوله تعالى:" in p.get_text():
                has_qawl = True

            if has_qawl:
                # Extraire les termes et le numéro de verset
                termes_span = p.find("span", class_="c3")
                verset_span = p.find("span", class_="c4")
                verset_number = verset_span.text.strip("[]") if verset_span else ""
                if verset_number:
                    previous_verset_number = verset_number  # Mettre à jour le numéro de verset précédent
                else:
                    verset_number = previous_verset_number  # Utiliser le numéro de verset précédent
                
                termes = termes_span.text.strip("{}") if termes_span else ""
                
                # Extraire le texte du verset sans "قوله تعالى:" et sans les termes
                texte = p.get_text().replace("قوله تعالى:", "").replace(f"{{{termes}}}", "").strip()
                # Retirer le numéro de verset en arabe (e.g., [١٣٦])
                if verset_span:
                    texte = texte.replace(f"[{verset_span.text.strip()}]", "").strip()
                
                # Extraire les narrateurs principaux avec leur annotation (n)
                narrateurs_text = ""
                for content in p.contents:
                    if isinstance(content, str):
                        narrateurs_text += content
                    elif content.name == "span" and "c5" not in content.get("class", []):
                        narrateurs_text += content.text
                    elif content.name == "span" and "c2" in content.get("class", []):
                        narrateurs_text += f"({content.text.strip()})"
                
                narrateurs_principaux = narrateurs_text.strip()
                
                # Extraire "Le reste des narrateurs" à partir de "والباقون"
                if "والباقون" in texte_entier:
                    text_after_baqoun = texte_entier.split("والباقون", 1)[1].strip()
                    reste_narrateurs = "والباقون" + text_after_baqoun
                else:
                    reste_narrateurs = ""
                
                # Créer une nouvelle entrée
                entry = {
                    "Page": nomor_halaman,
                    "Sourate": sourate,
                    "Numéro de verset": verset_number,
                    "Termes": f"{{{termes}}}" if termes else "",
                    "Narrateurs principaux": narrateurs_principaux,
                    "Reste des narrateurs": reste_narrateurs,
                    "Line Number": len(data) + 1,  # Incrémenter le numéro de ligne
                    "Annotations": ""  # À remplir après extraction des annotations
                }
                data.append(entry)
                previous_entry = entry  # Mettre à jour l'entrée précédente
            
            elif "والباقون" in texte_entier:
                # Extraire le reste des narrateurs et l'ajouter à l'entrée précédente
                text_after_baqoun = texte_entier.split("والباقون", 1)[1].strip()
                reste_narrateurs = "والباقون" + text_after_baqoun
                if previous_entry:
                    # Ajouter à "Reste des narrateurs" de l'entrée précédente
                    if previous_entry["Reste des narrateurs"]:
                        previous_entry["Reste des narrateurs"] += "<br>" + reste_narrateurs
                    else:
                        previous_entry["Reste des narrateurs"] = reste_narrateurs
                else:
                    # Si aucune entrée précédente, créer une nouvelle entrée avec "Reste des narrateurs"
                    entry = {
                        "Page": nomor_halaman,
                        "Sourate": sourate,
                        "Numéro de verset": previous_verset_number,
                        "Termes": "",
                        "Narrateurs principaux": "",
                        "Reste des narrateurs": reste_narrateurs,
                        "Line Number": len(data) + 1,
                        "Annotations": ""
                    }
                    data.append(entry)
                    previous_entry = entry
            
            else:
                # Vérifier si le paragraphe contient "قوله تعالى:" ailleurs
                if "قوله تعالى:" in p.get_text():
                    # Traiter comme un verset même s'il n'est pas dans 'c5'
                    termes_span = p.find("span", class_="c3")
                    verset_span = p.find("span", class_="c4")
                    verset_number = verset_span.text.strip("[]") if verset_span else ""
                    if verset_number:
                        previous_verset_number = verset_number  # Mettre à jour le numéro de verset précédent
                    else:
                        verset_number = previous_verset_number  # Utiliser le numéro de verset précédent
                    
                    termes = termes_span.text.strip("{}") if termes_span else ""
                    
                    # Extraire le texte du verset sans "قوله تعالى:" et sans les termes
                    texte = p.get_text().replace("قوله تعالى:", "").replace(f"{{{termes}}}", "").strip()
                    # Retirer le numéro de verset en arabe (e.g., [١٣٦])
                    if verset_span:
                        texte = texte.replace(f"[{verset_span.text.strip()}]", "").strip()
                    
                    # Extraire les narrateurs principaux avec leur annotation (n)
                    narrateurs_text = ""
                    for content in p.contents:
                        if isinstance(content, str):
                            narrateurs_text += content
                        elif content.name == "span" and "c5" not in content.get("class", []):
                            narrateurs_text += content.text
                        elif content.name == "span" and "c2" in content.get("class", []):
                            narrateurs_text += f"({content.text.strip()})"
                    
                    narrateurs_principaux = narrateurs_text.strip()
                    
                    # Extraire "Le reste des narrateurs" à partir de "والباقون"
                    if "والباقون" in texte_entier:
                        text_after_baqoun = texte_entier.split("والباقون", 1)[1].strip()
                        reste_narrateurs = "والباقون" + text_after_baqoun
                    else:
                        reste_narrateurs = ""
                    
                    # Créer une nouvelle entrée
                    entry = {
                        "Page": nomor_halaman,
                        "Sourate": sourate,
                        "Numéro de verset": verset_number,
                        "Termes": f"{{{termes}}}" if termes else "",
                        "Narrateurs principaux": narrateurs_principaux,
                        "Reste des narrateurs": reste_narrateurs,
                        "Line Number": len(data) + 1,
                        "Annotations": ""
                    }
                    data.append(entry)
                    previous_entry = entry  # Mettre à jour l'entrée précédente
                else:
                    # Si le paragraphe ne contient pas "قوله تعالى:", placer tout dans "Narrateurs principaux"
                    narrateurs_principaux = texte_entier
                    reste_narrateurs = ""
                    verset_number = previous_verset_number  # Utiliser le numéro de verset précédent
                    
                    # Créer une nouvelle entrée
                    entry = {
                        "Page": nomor_halaman,
                        "Sourate": sourate,
                        "Numéro de verset": verset_number,
                        "Termes": "",
                        "Narrateurs principaux": narrateurs_principaux,
                        "Reste des narrateurs": reste_narrateurs,
                        "Line Number": len(data) + 1,
                        "Annotations": ""
                    }
                    data.append(entry)
                    previous_entry = entry  # Mettre à jour l'entrée précédente

    # Extraire les annotations depuis le paragraphe .hamesh
        hamesh_paragraph = soup.find("p", class_="hamesh")
        annotations = {}
        if hamesh_paragraph:
            hamesh_text = hamesh_paragraph.get_text(separator="\n")
            lines = hamesh_text.split("\n")
            for line in lines:
                match = re.match(r'\((\d+)\)\s*(.*)', line)
                if match:
                    number = match.group(1)
                    annotation_text = match.group(2)
                    annotations[number] = annotation_text

    # Associer les annotations aux versets
        for entry in data:
            narrateurs = entry["Narrateurs principaux"]
            # Extraire les numéros d'annotation présents dans les narrateurs principaux
            numbers = re.findall(r'\((\d+)\)', narrateurs)
            annotation_texts = []
            for num in numbers:
                if num in annotations:
                    annotation_texts.append(f"({num}) {annotations[num]}")
            entry["Annotations"] = "<br>".join(annotation_texts)

        return data

# Fonction pour sauvegarder les données dans un fichier Excel
def save_to_excel(data, id_buku):
    df = pd.DataFrame(data)
    # Supprimer la colonne "Line Number" car elle n'est pas nécessaire dans le fichier final
    if "Line Number" in df.columns:
        df = df.drop(columns=["Line Number"])
    # Vérifier si le fichier existe déjà, sinon ajouter un suffixe
    file_name = get_available_filename(f"{id_buku}_result", ".xlsx")
    df.to_excel(file_name, index=False)  # Supprimer 'encoding'
    print(f"Fichier Excel enregistré sous : {file_name}")

# Fonction pour sauvegarder les données dans un fichier CSV
def save_to_csv(data, id_buku):
    df = pd.DataFrame(data)
    # Supprimer la colonne "Line Number" car elle n'est pas nécessaire dans le fichier final
    if "Line Number" in df.columns:
        df = df.drop(columns=["Line Number"])
    file_name = get_available_filename(f"{id_buku}_result", ".csv")
    with open(file_name, mode='w', encoding='utf-8', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=df.columns)
        writer.writeheader()
        writer.writerows(df.to_dict(orient='records'))
    print(f"Fichier CSV enregistré sous : {file_name}")

# Fonction pour sauvegarder les données dans un fichier JSON
def save_to_json(data, id_buku):
    import json
    df = pd.DataFrame(data)
    # Supprimer la colonne "Line Number" car elle n'est pas nécessaire dans le fichier final
    if "Line Number" in df.columns:
        df = df.drop(columns=["Line Number"])
    file_name = get_available_filename(f"{id_buku}_result", ".json")
    with open(file_name, 'w', encoding='utf-8') as json_file:
        json.dump(df.to_dict(orient='records'), json_file, ensure_ascii=False, indent=4)
    print(f"Fichier JSON enregistré sous : {file_name}")

# Fonction pour récupérer et sauvegarder un livre entier dans différents formats
def get_book(id_buku, nomor_halaman_awal, nomor_halaman_akhir, output_formats):
    content = []
    total_pages = nomor_halaman_akhir - nomor_halaman_awal + 1
    # Utilisation de concurrent.futures pour récupérer les pages en parallèle avec une barre de progression
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for i in range(nomor_halaman_awal, nomor_halaman_akhir + 1):
            futures.append(executor.submit(request_page, id_buku, i))
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=total_pages, desc="Scraping Pages"):
            page_data = future.result()
            if page_data:
                content.extend(page_data)
    
    # Trier le contenu par Page et Line Number
    df = pd.DataFrame(content)
    if not df.empty:
        df = df.sort_values(by=["Page", "Line Number"]).reset_index(drop=True)
    
    # Sauvegarder dans les formats spécifiés
    for output_format in output_formats:
        if output_format.lower() == 'excel':
            save_to_excel(df.to_dict(orient='records'), id_buku)
        elif output_format.lower() == 'csv':
            save_to_csv(df.to_dict(orient='records'), id_buku)
        elif output_format.lower() == 'json':
            save_to_json(df.to_dict(orient='records'), id_buku)
        else:
            print(f"Format {output_format} non supporté")

# Fonction principale pour poser des questions et récupérer les réponses
def main():
    print("Bienvenue dans le scraper de Maktabah Shamela!")
    
    # Poser des questions interactives
    try:
        id_buku = int(input("Entrez l'ID du livre à scraper : "))
        nomor_halaman_awal = int(input("Entrez le numéro de la première page à scraper : "))
        nomor_halaman_akhir = int(input("Entrez le numéro de la dernière page à scraper : "))
    except ValueError:
        print("Veuillez entrer des numéros valides.")
        return
    
    # Demander les formats de sortie
    formats_input = input("Entrez les formats de sortie (séparés par des espaces, ex: excel csv json) : ")
    output_formats = formats_input.split()
    
    # Appeler la fonction pour récupérer et sauvegarder le livre
    get_book(id_buku, nomor_halaman_awal, nomor_halaman_akhir, output_formats)
    
    input("Appuyez sur Enter pour fermer le programme...")

if __name__ == '__main__':
    main()
