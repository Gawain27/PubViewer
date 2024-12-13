import time
from telnetlib import EC

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from tempMail import EMail


class ProxyPatcher:

    def retrieveKey(self):

        # Initialize Chrome driver
        driver = webdriver.Chrome()

        # Navigate to the registration page
        driver.get('https://dashboard.scraperapi.com/signup')


        time.sleep(5)
        email = EMail()
        print(email.address)  # list of emails
        email_field = driver.find_element(By.ID, 'email')
        email_field.send_keys(email.address)

        password_field = driver.find_element(By.ID, 'password')
        password_field.send_keys('hv8rgfg84f78gd8fdsf')

        checkbox = driver.find_element(By.ID, 'terms')
        checkbox.click()

        captcha = driver.find_element(By.ID, 'recaptcha-token')
        captcha.click()
        time.sleep(2)

        # Submit the form
        button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Sign up with email')]"))
        )
        # Click the button
        button.click()
        print("Clicked the 'Sign up with email' button")

        # Wait for registration confirmation or error message
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, 'registration_confirmation'))
            )
            print("Registration successful!")
        except:
            print("Registration failed or timed out")

        # Close the browser
        driver.quit()

if __name__ == '__main__':
    ProxyPatcher().retrieveKey()
